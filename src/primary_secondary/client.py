"""
Client interattivo per il KV store con replica primary-secondary.

Si connette a un nodo via TCP e invia comandi testuali, una riga per comando.
Puo' essere usato sia verso un primary sia verso il secondario read-only.
"""

import argparse
import socket


def parse_args() -> argparse.Namespace:
    """Parsing argomenti CLI."""
    parser = argparse.ArgumentParser(description="Client per KV store primary-secondary")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6390)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    with socket.create_connection((args.host, args.port)) as connection:
        connection_file = connection.makefile("rwb")
        print(f"Connected to kv store on {args.host}:{args.port}")

        while True:
            try:
                line = input("kv> ")
            except EOFError:
                line = "QUIT"
                print()

            connection_file.write((line + "\n").encode("utf-8"))
            connection_file.flush()

            response = connection_file.readline()
            if not response:
                print("Connection closed by server.")
                break

            print(response.decode("utf-8", errors="replace").rstrip("\n"))

            if line.strip().upper() == "QUIT":
                break


if __name__ == "__main__":
    main()
