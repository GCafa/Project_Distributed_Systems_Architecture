"""
Client interattivo con Session Consistency per il KV Store.

Supporta due modalita':
  - STATELESS (default): il client traccia le versioni localmente e
    inietta MIN_VERSION automaticamente nelle GETV.
  - STATEFUL: il client aggiunge SESSION <client_id> a ogni comando,
    delegando il tracking delle versioni al coordinator.

ARCHITETTURA:
  Il client si connette al coordinator via TCP e invia comandi testuali.

  Modalita' STATELESS:
    Mantiene internamente una mappa: _session[key] = last_seen_version
    Ad ogni operazione di lettura (GETV) o scrittura (SET, CAS) riuscita,
    aggiorna la sessione con la versione restituita dal server.
    Nelle letture successive, aggiunge automaticamente MIN_VERSION.

  Modalita' STATEFUL:
    Appende SESSION <client_id> ai comandi SET, GETV, GET, CAS.
    Lo stato di sessione e' mantenuto dal coordinator lato server.
    Il client non traccia versioni localmente.

COMANDI DISPONIBILI:
  SET <key> <value>                  -> scrive e traccia la versione
  GETV <key>                         -> legge con garanzia di sessione
  CAS <key> <expected_version> <val> -> compare-and-swap e traccia
  GET <key>                          -> lettura semplice (senza versione)
  PING                               -> health check
  STATUS                             -> info cluster
  QUIT                               -> chiude la connessione
  SESSION                            -> mostra lo stato della sessione
  RESET_SESSION                      -> resetta la sessione (solo stateless)
"""

import argparse
import re
import socket


def parse_args() -> argparse.Namespace:
    """
    Parsing argomenti CLI.
    --host:      indirizzo del coordinator (default 127.0.0.1)
    --port:      porta del coordinator (default 6420)
    --mode:      stateless o stateful (default stateless)
    --client-id: identificativo del client (obbligatorio in modalita' stateful)
    """
    parser = argparse.ArgumentParser(
        description="Client interattivo con session consistency"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6420)
    parser.add_argument(
        "--mode", choices=["stateless", "stateful"], default="stateless",
        help="Modalita' di sessione: stateless (MIN_VERSION) o stateful (SESSION)",
    )
    parser.add_argument(
        "--client-id", default=None,
        help="Identificativo del client (obbligatorio in modalita' stateful)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Validazione: in modalita' stateful serve un client-id
    if args.mode == "stateful" and not args.client_id:
        print("ERROR: --client-id is required in stateful mode")
        return

    stateful = args.mode == "stateful"
    client_id = args.client_id

    # === STATO DI SESSIONE (solo modalita' stateless) ===
    # Mappa key -> ultima versione osservata (letta o scritta) per quella chiave.
    # Usata per iniettare MIN_VERSION automaticamente nelle GETV.
    # In modalita' stateful questo dizionario non viene usato.
    session: dict[str, int] = {}

    # Comandi che supportano il parametro SESSION in modalita' stateful
    session_commands = {"SET", "GET", "GETV", "CAS"}

    with socket.create_connection((args.host, args.port)) as connection:
        connection_file = connection.makefile("rwb")
        print(f"Connected to kv store on {args.host}:{args.port}")
        if stateful:
            print(f"Session consistency: STATEFUL (SESSION {client_id})")
            print("Session state is managed by the coordinator\n")
        else:
            print("Session consistency: STATELESS (MIN_VERSION auto-injected)")
            print("Type 'SESSION' to view session state, 'RESET_SESSION' to reset\n")

        while True:
            try:
                line = input("kv> ")
            except EOFError:
                line = "QUIT"
                print()

            stripped = line.strip()
            if not stripped:
                continue

            # --- Comandi locali (non inviati al server) ---

            # SESSION: mostra lo stato della sessione
            if stripped.upper() == "SESSION":
                if stateful:
                    print(f"  (session managed by coordinator, client_id={client_id})")
                else:
                    if not session:
                        print("  (session is empty)")
                    else:
                        for k, v in sorted(session.items()):
                            print(f"  {k} -> min_version={v}")
                continue

            # RESET_SESSION: resetta la sessione locale (solo stateless)
            if stripped.upper() == "RESET_SESSION":
                if stateful:
                    print("  (not available in stateful mode — session is on the coordinator)")
                else:
                    session.clear()
                    print("  session reset")
                continue

            # Estraggo il comando per la logica successiva
            tokens = stripped.split()
            command = tokens[0].upper() if tokens else ""

            if stateful:
                # --- Modalita' STATEFUL: append SESSION <client_id> ---
                # Il coordinator stateful usa SESSION per tracciare le versioni.
                # QUIT e PING/STATUS non supportano SESSION, li invio cosi' come sono.
                if command in session_commands:
                    stripped = f"{stripped} SESSION {client_id}"
                    print(f"  [session] -> {stripped}")
            else:
                # --- Modalita' STATELESS: iniezione automatica MIN_VERSION per GETV ---
                # Se il comando e' GETV <key> e non ha gia' MIN_VERSION,
                # e la sessione ha una versione per quella chiave,
                # aggiungo MIN_VERSION automaticamente.
                if (
                    len(tokens) == 2
                    and command == "GETV"
                    and tokens[1].upper() != "MIN_VERSION"
                ):
                    key = tokens[1]
                    if key in session:
                        stripped = f"GETV {key} MIN_VERSION {session[key]}"
                        print(f"  [session] -> {stripped}")

            # --- Invio al server ---
            connection_file.write((stripped + "\n").encode("utf-8"))
            connection_file.flush()
            response_raw = connection_file.readline()
            if not response_raw:
                print("Connection closed by server.")
                break
            response = response_raw.decode("utf-8", errors="replace").rstrip("\n")
            print(response)

            # --- Aggiornamento sessione dopo risposta OK (solo stateless) ---
            # In modalita' stateful il coordinator traccia le versioni lato server.
            if not stateful:
                # Pattern per GETV: "OK <value> version=<v>"
                getv_match = re.match(r"OK .+ version=(\d+)", response)
                # Pattern per SET/CAS: "OK version=<v> acks=<n>"
                set_match = re.match(r"OK version=(\d+) acks=\d+", response)

                if command in ("GETV", "GET") and getv_match:
                    # Aggiorno la sessione con la versione letta
                    key = tokens[1]
                    version = int(getv_match.group(1))
                    # max() garantisce monotonic reads: non torniamo mai indietro
                    session[key] = max(session.get(key, -1), version)

                elif command in ("SET", "CAS") and set_match:
                    # Aggiorno la sessione con la versione scritta
                    # Per SET: key e' tokens[1]
                    # Per CAS: key e' tokens[1]
                    key = tokens[1]
                    version = int(set_match.group(1))
                    session[key] = max(session.get(key, -1), version)

            # Controllo uscita basato sul comando originale (non su stripped
            # che potrebbe contenere SESSION appendato)
            if command == "QUIT":
                break


if __name__ == "__main__":
    main()
