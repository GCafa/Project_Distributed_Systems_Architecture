"""
Client interattivo STATELESS per il KV store con quorum.

Il client mantiene localmente lo stato di sessione come:
    session[key] = last_seen_version

Quando legge una chiave gia' vista, aggiunge automaticamente
`MIN_VERSION <last_seen_version>` a GETV e GET. In questo modo il
coordinator resta stateless: non conserva client_id ne' cronologia delle
sessioni, ma rifiuta letture piu' vecchie del minimo dichiarato dal client.
"""

import argparse
import re
import socket


def parse_args() -> argparse.Namespace:
    """
    Parsing argomenti CLI.
    --host: indirizzo del coordinator stateless
    --port: porta del coordinator stateless
    """
    parser = argparse.ArgumentParser(
        description="Client stateless con session consistency via MIN_VERSION"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6420)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Stato locale di sessione: key -> versione massima osservata.
    session: dict[str, int] = {}

    with socket.create_connection((args.host, args.port)) as connection:
        connection_file = connection.makefile("rwb")
        print(f"Connected to stateless kv store on {args.host}:{args.port}")
        print("Session consistency: STATELESS (MIN_VERSION auto-injected)")
        print("Type 'SESSION' to view session state, 'RESET_SESSION' to reset\n")

        while True:
            try:
                line = input("kv-stateless> ")
            except EOFError:
                line = "QUIT"
                print()

            stripped = line.strip()
            if not stripped:
                continue

            if stripped.upper() == "SESSION":
                if not session:
                    print("  (session is empty)")
                else:
                    for key, version in sorted(session.items()):
                        print(f"  {key} -> min_version={version}")
                continue

            if stripped.upper() == "RESET_SESSION":
                session.clear()
                print("  session reset")
                continue

            tokens = stripped.split()
            command = tokens[0].upper() if tokens else ""

            # Iniezione automatica del vincolo di sessione sulle letture.
            if (
                len(tokens) == 2
                and command in {"GETV", "GET"}
                and tokens[1] in session
            ):
                key = tokens[1]
                stripped = f"{command} {key} MIN_VERSION {session[key]}"
                print(f"  [session] -> {stripped}")

            connection_file.write((stripped + "\n").encode("utf-8"))
            connection_file.flush()
            response_raw = connection_file.readline()
            if not response_raw:
                print("Connection closed by server.")
                break

            response = response_raw.decode("utf-8", errors="replace").rstrip("\n")
            print(response)

            # Aggiorna la sessione solo quando la risposta contiene una versione.
            getv_match = re.match(r"OK .+ version=(\d+)", response)
            write_match = re.match(r"OK version=(\d+) acks=\d+", response)

            if command == "GETV" and len(tokens) >= 2 and getv_match:
                key = tokens[1]
                version = int(getv_match.group(1))
                session[key] = max(session.get(key, -1), version)
            elif command in {"SET", "CAS"} and len(tokens) >= 2 and write_match:
                key = tokens[1]
                version = int(write_match.group(1))
                session[key] = max(session.get(key, -1), version)

            if command == "QUIT":
                break


if __name__ == "__main__":
    main()
