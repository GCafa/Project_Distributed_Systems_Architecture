"""
Client interattivo STATEFUL per il KV store con quorum.

Il client non conserva versioni localmente. Per ogni comando dati aggiunge
`SESSION <client_id>` e delega al coordinator stateful la memoria della
sessione, cioe' la mappa client_id -> key -> last_seen_version.
"""

import argparse
import socket


def parse_args() -> argparse.Namespace:
    """
    Parsing argomenti CLI.
    --host: indirizzo del coordinator stateful
    --port: porta del coordinator stateful
    --client-id: identificativo della sessione lato server
    """
    parser = argparse.ArgumentParser(
        description="Client stateful con session consistency via SESSION"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6430)
    parser.add_argument("--client-id", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session_commands = {"SET", "GET", "GETV", "CAS"}

    with socket.create_connection((args.host, args.port)) as connection:
        connection_file = connection.makefile("rwb")
        print(f"Connected to stateful kv store on {args.host}:{args.port}")
        print(f"Session consistency: STATEFUL (SESSION {args.client_id})")
        print("Type 'SESSION' to show the active client id\n")

        while True:
            try:
                line = input("kv-stateful> ")
            except EOFError:
                line = "QUIT"
                print()

            stripped = line.strip()
            if not stripped:
                continue

            if stripped.upper() == "SESSION":
                print(f"  client_id={args.client_id}")
                continue

            tokens = stripped.split()
            command = tokens[0].upper() if tokens else ""

            has_session_suffix = (
                len(tokens) >= 3 and tokens[-2].upper() == "SESSION"
            )
            if command in session_commands and not has_session_suffix:
                stripped = f"{stripped} SESSION {args.client_id}"
                print(f"  [session] -> {stripped}")

            connection_file.write((stripped + "\n").encode("utf-8"))
            connection_file.flush()
            response_raw = connection_file.readline()
            if not response_raw:
                print("Connection closed by server.")
                break

            response = response_raw.decode("utf-8", errors="replace").rstrip("\n")
            print(response)

            if command == "QUIT":
                break


if __name__ == "__main__":
    main()
