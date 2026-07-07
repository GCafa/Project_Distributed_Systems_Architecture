"""
Client interattivo con Session Consistency per il KV Store.

ARCHITETTURA:
  Il client si connette al coordinator via TCP e invia comandi testuali.
  Per garantire session consistency, mantiene internamente una mappa:
    _session[key] = last_seen_version

  Ad ogni operazione di lettura (GETV) o scrittura (SET, CAS) riuscita,
  il client aggiorna la sessione con la versione restituita dal server.

  Nelle letture successive, il client aggiunge automaticamente
  MIN_VERSION alla richiesta, cosi' il coordinator puo' rifiutare
  risposte stale.

APPROCCIO STATELESS:
  Lo stato di sessione e' interamente lato client.
  Il server non mantiene alcuna informazione sulle sessioni.
  Questo scala meglio rispetto all'approccio stateful.

COMANDI DISPONIBILI:
  SET <key> <value>                  -> scrive e traccia la versione
  GETV <key>                         -> legge con MIN_VERSION automatico
  CAS <key> <expected_version> <val> -> compare-and-swap e traccia
  GET <key>                          -> lettura semplice (senza versione)
  PING                               -> health check
  STATUS                             -> info cluster
  QUIT                               -> chiude la connessione
  SESSION                            -> mostra lo stato della sessione locale
  RESET_SESSION                      -> resetta la sessione (debug)
"""

import argparse
import re
import socket


def parse_args() -> argparse.Namespace:
    """
    Parsing argomenti CLI.
    --host: indirizzo del coordinator (default 127.0.0.1)
    --port: porta del coordinator (default 6420)
    """
    parser = argparse.ArgumentParser(
        description="Client interattivo con session consistency"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6420)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # === STATO DI SESSIONE ===
    # Mappa key -> ultima versione osservata (letta o scritta) per quella chiave.
    # Usata per iniettare MIN_VERSION automaticamente nelle GETV.
    session: dict[str, int] = {}

    with socket.create_connection((args.host, args.port)) as connection:
        connection_file = connection.makefile("rwb")
        print(f"Connected to kv store on {args.host}:{args.port}")
        print("Session consistency is ACTIVE (MIN_VERSION auto-injected)")
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

            # SESSION: mostra lo stato della sessione locale
            if stripped.upper() == "SESSION":
                if not session:
                    print("  (session is empty)")
                else:
                    for k, v in sorted(session.items()):
                        print(f"  {k} -> min_version={v}")
                continue

            # RESET_SESSION: resetta la sessione (utile per testing/debug)
            if stripped.upper() == "RESET_SESSION":
                session.clear()
                print("  session reset")
                continue

            # --- Iniezione automatica MIN_VERSION per GETV ---
            # Se il comando e' GETV <key> e non ha gia' MIN_VERSION,
            # e la sessione ha una versione per quella chiave,
            # aggiungo MIN_VERSION automaticamente.
            tokens = stripped.split()
            if (
                len(tokens) == 2
                and tokens[0].upper() == "GETV"
                and tokens[1].upper() != "MIN_VERSION"
            ):
                key = tokens[1]
                if key in session:
                    # Inietto MIN_VERSION dalla sessione
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

            # --- Aggiornamento sessione dopo risposta OK ---
            # Parso la risposta per estrarre la versione e aggiornare la sessione.

            # Pattern per GETV: "OK <value> version=<v>"
            getv_match = re.match(r"OK .+ version=(\d+)", response)
            # Pattern per SET/CAS: "OK version=<v> acks=<n>"
            set_match = re.match(r"OK version=(\d+) acks=\d+", response)

            if tokens[0].upper() in ("GETV", "GET") and getv_match:
                # Aggiorno la sessione con la versione letta
                key = tokens[1]
                version = int(getv_match.group(1))
                # max() garantisce monotonic reads: non torniamo mai indietro
                session[key] = max(session.get(key, -1), version)

            elif tokens[0].upper() in ("SET", "CAS") and set_match:
                # Aggiorno la sessione con la versione scritta
                # Per SET: key e' tokens[1]
                # Per CAS: key e' tokens[1]
                key = tokens[1]
                version = int(set_match.group(1))
                session[key] = max(session.get(key, -1), version)

            if stripped.upper() == "QUIT":
                break


if __name__ == "__main__":
    main()
