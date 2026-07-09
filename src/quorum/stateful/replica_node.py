"""
Replica Node per il KV Store con Quorum e Session Consistency.

ARCHITETTURA:
  Ogni replica e' un server TCP che mantiene in memoria un dizionario
  chiave -> {value, version}. Il coordinator comunica via JSON su TCP.

PROTOCOLLO INTERNO (coordinator -> replica):
  - "read"   : legge il valore di una chiave
  - "write"  : scrive un valore con una versione (accetta solo se >= corrente)
  - "cas"    : compare-and-swap atomico
  - "status" : restituisce la lista delle chiavi memorizzate

CONCORRENZA:
  Un threading.Lock protegge _data da accessi concorrenti.

VERSIONING:
  Ogni valore ha un intero monotono crescente (version).
  Una write e' accettata solo se incoming_version >= current_version,
  cosi' il read repair non sovrascrive dati piu' recenti.
"""

import argparse
import json
import socket
import threading
from datetime import datetime


def log(message: str) -> None:
    """Stampa un messaggio di log con timestamp e nome del thread."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread_name = threading.current_thread().name
    print(f"[{timestamp}] [{thread_name}] {message}")


def parse_args() -> argparse.Namespace:
    """
    Parsing argomenti CLI.
    --node-id: id univoco della replica (es. "R0")
    --host:    indirizzo di ascolto (default 127.0.0.1)
    --port:    porta TCP (obbligatorio)
    """
    parser = argparse.ArgumentParser(description="Replica node per KV store")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    return parser.parse_args()


class ReplicaNode:
    """
    Nodo replica del KV store.

    _data[key] = {"value": str, "version": int}

    Supporta: read, write, cas, status.
    """

    def __init__(self, node_id: str) -> None:
        self._node_id = node_id
        # Lock per accesso thread-safe al dizionario
        self._lock = threading.Lock()
        # Dizionario principale: key -> {"value": str, "version": int}
        self._data: dict[str, dict[str, object]] = {}

    def handle_message(self, raw_line: str) -> str:
        """Processa un messaggio JSON e restituisce la risposta JSON."""
        try:
            message = json.loads(raw_line)
        except json.JSONDecodeError:
            return json.dumps({"status": "ERR", "error": "invalid json"})

        message_type = message.get("type")

        # === READ: legge chiave, restituisce value+version o found=False ===
        if message_type == "read":
            key = str(message.get("key", ""))
            if not key:
                return json.dumps({"status": "ERR", "error": "missing key"})
            with self._lock:
                record = self._data.get(key)
            if record is None:
                return json.dumps(
                    {"status": "OK", "found": False, "node": self._node_id, "key": key}
                )
            return json.dumps({
                "status": "OK", "found": True, "node": self._node_id,
                "key": key, "value": record["value"], "version": record["version"],
            })

        # === WRITE: scrive se incoming_version >= current_version ===
        # Garantisce che il read repair non sovrascriva dati piu' nuovi.
        if message_type == "write":
            key = str(message.get("key", ""))
            value = str(message.get("value", ""))
            version = int(message.get("version", 0))
            if not key:
                return json.dumps({"status": "ERR", "error": "missing key"})
            with self._lock:
                current = self._data.get(key)
                current_version = int(current["version"]) if current else -1
                if version >= current_version:
                    self._data[key] = {"value": value, "version": version}
            return json.dumps(
                {"status": "ACK", "node": self._node_id, "version": version}
            )

        # === CAS: compare-and-swap atomico ===
        # Scrive solo se la versione corrente == expected_version.
        # Permette aggiornamenti ottimistici: "scrivi X solo se nessuno
        # ha modificato la chiave da quando l'ho letta".
        if message_type == "cas":
            key = str(message.get("key", ""))
            expected_version = int(message.get("expected_version", -1))
            new_value = str(message.get("new_value", ""))
            new_version = int(message.get("new_version", 0))
            if not key:
                return json.dumps({"status": "ERR", "error": "missing key"})
            with self._lock:
                current = self._data.get(key)
                if current is None:
                    return json.dumps({
                        "status": "ERR", "error": "not_found",
                        "node": self._node_id, "key": key,
                    })
                current_version = int(current["version"])
                if current_version != expected_version:
                    return json.dumps({
                        "status": "ERR", "error": "version_mismatch",
                        "node": self._node_id, "key": key,
                        "current_version": current_version,
                    })
                self._data[key] = {"value": new_value, "version": new_version}
            return json.dumps(
                {"status": "ACK", "node": self._node_id, "version": new_version}
            )

        # === STATUS: elenca chiavi memorizzate (debug) ===
        if message_type == "status":
            with self._lock:
                keys = sorted(self._data.keys())
            return json.dumps(
                {"status": "OK", "node": self._node_id, "keys": keys}
            )

        return json.dumps({"status": "ERR", "error": "unknown type"})


def handle_connection(
    connection: socket.socket,
    address: tuple[str, int],
    replica: ReplicaNode,
) -> None:
    """
    Gestisce una connessione TCP dal coordinator.
    Protocollo: una riga JSON in -> una riga JSON out.
    """
    log(f"connection from {address[0]}:{address[1]}")
    with connection:
        connection_file = connection.makefile("rwb")
        while True:
            raw_line = connection_file.readline()
            if not raw_line:
                break
            response = replica.handle_message(
                raw_line.decode("utf-8", errors="replace").strip()
            )
            connection_file.write((response + "\n").encode("utf-8"))
            connection_file.flush()
            log(f"response: {response}")


def serve() -> None:
    """
    Entry point: parsa argomenti, crea replica, apre socket TCP.
    Ogni connessione e' gestita in un thread daemon separato.
    """
    args = parse_args()
    replica = ReplicaNode(args.node_id)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # SO_REUSEADDR: permette riavvio rapido senza aspettare rilascio porta
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((args.host, args.port))
        server_socket.listen()
        log(f"replica {args.node_id} listening on {args.host}:{args.port}")

        while True:
            connection, address = server_socket.accept()
            threading.Thread(
                target=handle_connection,
                args=(connection, address, replica),
                daemon=True,
            ).start()


if __name__ == "__main__":
    serve()
