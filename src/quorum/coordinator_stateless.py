"""
Coordinator con Quorum, Session Consistency (MIN_VERSION) e Read Repair.

Basato sul coordinator del laboratorio quorum_cluster, esteso con:
  - GETV <key> [MIN_VERSION <v>]   lettura con garanzia di sessione
  - GET <key> [MIN_VERSION <v>]    come GETV ma senza version= nella risposta
  - CAS <key> <expected_ver> <val> compare-and-swap versionato
  - Read Repair                    aggiorna in background le repliche stale

PROTOCOLLO CLIENT -> COORDINATOR (testuale, una riga per comando):
  PING                              -> OK PONG
  STATUS                            -> OK N=3 R=2 W=2
  SET <key> <value>                 -> OK version=<v> acks=<n>
  GETV <key> [MIN_VERSION <v>]      -> OK <value> version=<v>
  GET <key> [MIN_VERSION <v>]       -> OK <value>
  CAS <key> <expected_ver> <value>  -> OK version=<v> acks=<n>
  QUIT                              -> OK BYE

SESSION CONSISTENCY (Homework 5):
  MIN_VERSION e' un parametro opzionale per GETV e GET.
  Se specificato, il coordinator verifica che la versione migliore
  trovata nelle repliche sia >= MIN_VERSION. Strategia:
    1. Legge dal quorum R
    2. Se soddisfa il vincolo -> OK, read repair in background
    3. Se no, legge dalle repliche rimanenti (fallback)
    4. Se ancora non soddisfa -> ERR stale min_version=<v> best=<b>
  Questo garantisce monotonic reads e read-your-writes lato client,
  a patto che il client tracci le versioni e le invii correttamente.

READ REPAIR:
  Quando il coordinator legge da piu' repliche e trova versioni
  diverse, dopo aver risposto al client, aggiorna le repliche
  stale con la versione piu' recente (in background).
"""

import argparse
import json
import socket
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Callable


# Tipo per gli handler dei comandi: riceve argomenti, restituisce (risposta, should_close)
CommandHandler = Callable[[str], tuple[str, bool]]


def log(message: str) -> None:
    """Stampa un messaggio di log con timestamp e nome del thread."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread_name = threading.current_thread().name
    print(f"[{timestamp}] [{thread_name}] {message}")


@dataclass(frozen=True)
class ReplicaEndpoint:
    """Indirizzo di una replica (host:port). Frozen = immutabile e hashable."""
    host: str
    port: int


def parse_args() -> argparse.Namespace:
    """
    Parsing argomenti CLI.
    --host/--port:     indirizzo del coordinator
    --read-quorum:     numero minimo di repliche da leggere (R)
    --write-quorum:    numero minimo di ACK per considerare la write riuscita (W)
    --replicas:        lista di endpoint replica nel formato host:port
    """
    parser = argparse.ArgumentParser(description="Quorum coordinator con session consistency")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6420)
    parser.add_argument("--read-quorum", type=int, default=2)
    parser.add_argument("--write-quorum", type=int, default=2)
    parser.add_argument(
        "--replicas", nargs="+",
        default=["127.0.0.1:6421", "127.0.0.1:6422", "127.0.0.1:6423"],
    )
    return parser.parse_args()


class QuorumCoordinator:
    """
    Coordinator centrale del KV store con quorum, session consistency e read repair.

    Responsabilita':
    - Ricevere comandi testuali dai client
    - Tradurli in RPC JSON verso le repliche
    - Applicare la logica di quorum (R letture, W scritture)
    - Verificare il vincolo MIN_VERSION per session consistency
    - Eseguire read repair sulle repliche stale
    """

    def __init__(
        self,
        replicas: list[ReplicaEndpoint],
        read_quorum: int,
        write_quorum: int,
    ) -> None:
        self._replicas = replicas
        self._read_quorum = read_quorum
        self._write_quorum = write_quorum
        # Mappa comando -> handler
        self._handlers: dict[str, CommandHandler] = {
            "PING": self._handle_ping,
            "STATUS": self._handle_status,
            "SET": self._handle_set,
            "GET": self._handle_get,
            "GETV": self._handle_getv,
            "CAS": self._handle_cas,
            "QUIT": self._handle_quit,
        }

    def execute(self, line: str) -> tuple[str, bool]:
        """
        Esegue un comando testuale. Restituisce (risposta, should_close).
        should_close=True indica al server di chiudere la connessione.
        """
        stripped = line.strip()
        if not stripped:
            return "ERR empty command", False
        command, *rest = stripped.split(" ", 1)
        command = command.upper()
        argument_blob = rest[0] if rest else ""
        handler = self._handlers.get(command)
        if handler is None:
            return "ERR unknown command", False
        return handler(argument_blob)

    # =====================================================================
    # RPC: comunicazione coordinator -> replica
    # =====================================================================

    def _rpc(
        self, replica: ReplicaEndpoint, message: dict[str, object]
    ) -> dict[str, object] | None:
        """
        Invia un messaggio JSON a una replica e restituisce la risposta.

        Apre una connessione TCP, invia il JSON + newline, legge la risposta.
        Timeout di 1 secondo. Se la replica e' irraggiungibile restituisce None.
        """
        try:
            with socket.create_connection(
                (replica.host, replica.port), timeout=1.0
            ) as connection:
                connection_file = connection.makefile("rwb")
                payload = json.dumps(message) + "\n"
                connection_file.write(payload.encode("utf-8"))
                connection_file.flush()
                response = (
                    connection_file.readline()
                    .decode("utf-8", errors="replace")
                    .strip()
                )
        except OSError:
            return None
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return None

    # =====================================================================
    # Logica di quorum per le letture
    # =====================================================================

    def _collect_reads(
        self, key: str, limit: int | None = None
    ) -> list[tuple[ReplicaEndpoint, dict[str, object]]]:
        """
        Interroga le repliche per leggere una chiave.

        Restituisce una lista di (replica_endpoint, response_dict).
        Tenere traccia della replica permette di fare read repair sulle
        repliche stale.

        Se limit e' specificato, si ferma dopo aver raccolto limit risposte.
        Se limit=None, interroga TUTTE le repliche.
        """
        results: list[tuple[ReplicaEndpoint, dict[str, object]]] = []
        for replica in self._replicas:
            response = self._rpc(replica, {"type": "read", "key": key})
            if response is None or response.get("status") != "OK":
                continue
            results.append((replica, response))
            if limit is not None and len(results) >= limit:
                break
        return results

    def _collect_reads_remaining(
        self,
        key: str,
        already_read: list[tuple[ReplicaEndpoint, dict[str, object]]],
    ) -> list[tuple[ReplicaEndpoint, dict[str, object]]]:
        """
        Legge dalle repliche NON ancora interrogate.

        Evita di interrogare due volte la stessa replica durante il
        fallback o il read repair.
        """
        already_contacted = {replica for replica, _ in already_read}
        results: list[tuple[ReplicaEndpoint, dict[str, object]]] = []
        for replica in self._replicas:
            if replica in already_contacted:
                continue
            response = self._rpc(replica, {"type": "read", "key": key})
            if response is None or response.get("status") != "OK":
                continue
            results.append((replica, response))
        return results

    def _highest_version(
        self, reads: list[tuple[ReplicaEndpoint, dict[str, object]]]
    ) -> tuple[int, str | None]:
        """
        Trova la versione piu' alta tra le risposte delle repliche.

        Restituisce (best_version, best_value).
        Se nessuna replica ha trovato la chiave, restituisce (-1, None).
        """
        best_version = -1
        best_value: str | None = None
        for _replica, read in reads:
            if not bool(read.get("found", False)):
                continue
            version = int(read.get("version", -1))
            if version > best_version:
                best_version = version
                best_value = str(read.get("value", ""))
        return best_version, best_value

    # =====================================================================
    # Read Repair
    # =====================================================================

    def _do_read_repair(
        self,
        key: str,
        best_version: int,
        best_value: str,
        reads: list[tuple[ReplicaEndpoint, dict[str, object]]],
    ) -> None:
        """
        Aggiorna le repliche stale con la versione piu' recente.

        Per ogni replica il cui valore letto ha version < best_version,
        invia una write. La replica accetta solo se incoming_version >=
        current_version, quindi e' safe: non sovrascrive mai un valore
        piu' recente arrivato nel frattempo.

        Eseguito in un thread separato per non bloccare la risposta al client.
        """
        for replica, read in reads:
            read_version = int(read.get("version", -1)) if read.get("found") else -1
            if read_version < best_version:
                log(
                    f"read repair: {replica.host}:{replica.port} "
                    f"version={read_version} -> {best_version}"
                )
                self._rpc(
                    replica,
                    {
                        "type": "write",
                        "key": key,
                        "value": best_value,
                        "version": best_version,
                    },
                )

    # =====================================================================
    # Lettura session-aware (MIN_VERSION + fallback + read repair)
    # =====================================================================

    def _session_aware_read(
        self, key: str, min_version: int | None
    ) -> tuple[int, str | None, list[tuple[ReplicaEndpoint, dict[str, object]]]]:
        """
        Lettura con garanzia di sessione e read repair.

        Strategia:
        1. Legge dal quorum R
        2. Se la versione migliore >= min_version -> OK
           - legge le repliche rimanenti per read repair
        3. Se no, legge dalle repliche rimanenti (fallback)
        4. Se ancora non soddisfa -> la versione resta < min_version

        Restituisce (version, value, all_reads).
        """
        # Passo 1: lettura quorum R
        reads = self._collect_reads(key, limit=self._read_quorum)
        version, value = self._highest_version(reads)

        # Passo 2: vincolo soddisfatto o nessun vincolo
        if min_version is None or version >= min_version:
            if version >= 0 and value is not None:
                # Legge le repliche rimanenti per read repair
                remaining = self._collect_reads_remaining(key, reads)
                all_reads = reads + remaining
                # Controlla se una replica rimanente ha una versione migliore
                all_version, all_value = self._highest_version(all_reads)
                if all_version > version:
                    version, value = all_version, all_value
                # Read repair in background
                threading.Thread(
                    target=self._do_read_repair,
                    args=(key, version, value, all_reads),
                    daemon=True,
                ).start()
                return version, value, all_reads
            return version, value, reads

        # Passo 3: fallback — legge le repliche non ancora contattate
        log(f"session fallback: version={version} < min_version={min_version} for key={key}")
        remaining = self._collect_reads_remaining(key, reads)
        all_reads = reads + remaining
        version, value = self._highest_version(all_reads)

        # Read repair anche in caso di fallback
        if version >= 0 and value is not None:
            threading.Thread(
                target=self._do_read_repair,
                args=(key, version, value, all_reads),
                daemon=True,
            ).start()

        return version, value, all_reads

    # =====================================================================
    # Handler dei comandi
    # =====================================================================

    def _handle_ping(self, argument_blob: str) -> tuple[str, bool]:
        """PING -> OK PONG. Health check."""
        if argument_blob.strip():
            return "ERR usage: PING", False
        return "OK PONG", False

    def _handle_status(self, argument_blob: str) -> tuple[str, bool]:
        """STATUS -> OK N=<n> R=<r> W=<w>. Info sul cluster."""
        if argument_blob.strip():
            return "ERR usage: STATUS", False
        return (
            f"OK N={len(self._replicas)} R={self._read_quorum} W={self._write_quorum}",
            False,
        )

    def _handle_set(self, argument_blob: str) -> tuple[str, bool]:
        """
        SET <key> <value>

        1. Legge la versione corrente da TUTTE le repliche
        2. Calcola next_version = max_version + 1
        3. Scrive su tutte le repliche con next_version
        4. Se almeno W repliche confermano (ACK), restituisce OK
        """
        parts = argument_blob.split(" ", 1)
        if len(parts) != 2 or not parts[0]:
            return "ERR usage: SET <key> <value>", False
        key, value = parts

        # Leggo da tutte le repliche per trovare la versione corrente
        reads = self._collect_reads(key)
        current_version, _ = self._highest_version(reads)
        next_version = current_version + 1

        # Scrivo su tutte le repliche, conto gli ACK
        acknowledgements = 0
        for replica in self._replicas:
            response = self._rpc(
                replica,
                {"type": "write", "key": key, "value": value, "version": next_version},
            )
            if response is not None and response.get("status") == "ACK":
                acknowledgements += 1
                if acknowledgements >= self._write_quorum:
                    return (
                        f"OK version={next_version} acks={acknowledgements}",
                        False,
                    )

        return f"ERR write quorum not reached acks={acknowledgements}", False

    def _handle_getv(self, argument_blob: str) -> tuple[str, bool]:
        """
        GETV <key> [MIN_VERSION <v>]

        Lettura con session consistency e read repair.
        Se MIN_VERSION e' specificato e la versione migliore e' inferiore,
        restituisce ERR stale.
        """
        tokens = argument_blob.strip().split()
        if not tokens:
            return "ERR usage: GETV <key> [MIN_VERSION <v>]", False

        key = tokens[0]
        min_version: int | None = None

        if len(tokens) == 3 and tokens[1].upper() == "MIN_VERSION":
            try:
                min_version = int(tokens[2])
            except ValueError:
                return "ERR MIN_VERSION must be an integer", False
        elif len(tokens) != 1:
            return "ERR usage: GETV <key> [MIN_VERSION <v>]", False

        version, value, reads = self._session_aware_read(key, min_version)

        if len(reads) < self._read_quorum:
            return f"ERR read quorum not reached responses={len(reads)}", False

        if version < 0 or value is None:
            return "NOT_FOUND", False

        if min_version is not None and version < min_version:
            return f"ERR stale min_version={min_version} best={version}", False

        return f"OK {value} version={version}", False

    def _handle_get(self, argument_blob: str) -> tuple[str, bool]:
        """
        GET <key> [MIN_VERSION <v>]

        Come GETV ma senza version= nella risposta di successo.
        """
        tokens = argument_blob.strip().split()
        if not tokens:
            return "ERR usage: GET <key> [MIN_VERSION <v>]", False

        key = tokens[0]
        min_version: int | None = None

        if len(tokens) == 3 and tokens[1].upper() == "MIN_VERSION":
            try:
                min_version = int(tokens[2])
            except ValueError:
                return "ERR MIN_VERSION must be an integer", False
        elif len(tokens) != 1:
            return "ERR usage: GET <key> [MIN_VERSION <v>]", False

        version, value, reads = self._session_aware_read(key, min_version)

        if len(reads) < self._read_quorum:
            return f"ERR read quorum not reached responses={len(reads)}", False

        if version < 0 or value is None:
            return "NOT_FOUND", False

        if min_version is not None and version < min_version:
            return f"ERR stale min_version={min_version} best={version}", False

        return f"OK {value}", False

    def _handle_cas(self, argument_blob: str) -> tuple[str, bool]:
        """
        CAS <key> <expected_version> <new_value>

        Compare-And-Swap:
        1. Legge da tutte le repliche per trovare la versione corrente
        2. Verifica che current_version == expected_version
        3. Scrive new_value con version = expected + 1
        4. Richiede almeno W ACK

        Usa "type": "write" verso le repliche (la replica accetta
        solo se incoming_version >= current_version).
        """
        parts = argument_blob.strip().split(" ", 2)
        if len(parts) != 3:
            return "ERR usage: CAS <key> <expected_version> <new_value>", False

        key = parts[0]
        try:
            expected_version = int(parts[1])
        except ValueError:
            return "ERR expected_version must be an integer", False
        new_value = parts[2]

        # Leggo da tutte le repliche per trovare la versione corrente
        reads = self._collect_reads(key)
        if len(reads) < self._read_quorum:
            return f"ERR read quorum not reached responses={len(reads)}", False

        current_version, _ = self._highest_version(reads)

        if current_version < 0:
            return "ERR not_found", False
        if current_version != expected_version:
            return f"ERR version_mismatch current={current_version}", False

        # Scrive la nuova versione su tutte le repliche
        new_version = expected_version + 1
        acknowledgements = 0
        for replica in self._replicas:
            response = self._rpc(
                replica,
                {
                    "type": "write",
                    "key": key,
                    "value": new_value,
                    "version": new_version,
                },
            )
            if response is not None and response.get("status") == "ACK":
                acknowledgements += 1
                if acknowledgements >= self._write_quorum:
                    return (
                        f"OK version={new_version} acks={acknowledgements}",
                        False,
                    )

        return f"ERR write quorum not reached acks={acknowledgements}", False

    def _handle_quit(self, argument_blob: str) -> tuple[str, bool]:
        """QUIT -> OK BYE. Chiude la connessione."""
        if argument_blob.strip():
            return "ERR usage: QUIT", False
        return "OK BYE", True


# =====================================================================
# Server TCP
# =====================================================================

def handle_client(
    connection: socket.socket,
    address: tuple[str, int],
    coordinator: QuorumCoordinator,
) -> None:
    """
    Gestisce una connessione client.
    Loop: legge una riga -> esegue il comando -> scrive la risposta.
    """
    log(f"client connection from {address[0]}:{address[1]}")
    with connection:
        connection_file = connection.makefile("rwb")
        while True:
            raw_line = connection_file.readline()
            if not raw_line:
                break
            line = raw_line.decode("utf-8", errors="replace")
            log(f"request: {line.rstrip()}")
            response, should_close = coordinator.execute(line)
            connection_file.write((response + "\n").encode("utf-8"))
            connection_file.flush()
            log(f"response: {response}")
            if should_close:
                break


def serve() -> None:
    """
    Entry point del coordinator.
    1. Parsa argomenti
    2. Costruisce la lista di ReplicaEndpoint
    3. Crea il QuorumCoordinator
    4. Apre socket TCP e accetta client in thread separati
    """
    args = parse_args()
    replicas = [
        ReplicaEndpoint(
            host=entry.split(":", 1)[0], port=int(entry.split(":", 1)[1])
        )
        for entry in args.replicas
    ]
    coordinator = QuorumCoordinator(replicas, args.read_quorum, args.write_quorum)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((args.host, args.port))
        server_socket.listen()
        log(
            f"session coordinator listening on {args.host}:{args.port} "
            f"N={len(replicas)} R={args.read_quorum} W={args.write_quorum}"
        )

        while True:
            connection, address = server_socket.accept()
            threading.Thread(
                target=handle_client,
                args=(connection, address, coordinator),
                daemon=True,
            ).start()


if __name__ == "__main__":
    serve()
