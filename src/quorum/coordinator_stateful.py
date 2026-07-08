"""
Coordinator con Quorum, Session Consistency STATEFUL (SESSION) e Read Repair.

Variante stateful del coordinator: il server mantiene una mappa
    session[client_id][key] = last_seen_version
e la usa per garantire read-your-writes e monotonic reads senza che
il client debba inviare MIN_VERSION a ogni richiesta.

PROTOCOLLO CLIENT -> COORDINATOR (testuale, una riga per comando):
  PING                                       -> OK PONG
  STATUS                                     -> OK N=3 R=2 W=2 sessions=<n>
  SET <key> <value> [SESSION <client_id>]    -> OK version=<v> acks=<n>
  GETV <key> [SESSION <client_id>]           -> OK <value> version=<v>
  GET <key> [SESSION <client_id>]            -> OK <value>
  CAS <key> <ver> <val> [SESSION <client_id>]-> OK version=<v> acks=<n>
  QUIT                                       -> OK BYE

SESSION CONSISTENCY (Homework 5):
  Se il client specifica SESSION <client_id>, il coordinator:
  - Dopo ogni SET/CAS riuscito: salva session[client_id][key] = new_version
  - Su ogni GETV/GET: verifica che la versione letta sia >= session[client_id][key]
  - Se la versione e' insufficiente: fallback a tutte le repliche
  - Se ancora insufficiente: ERR stale session_min=<v> best=<b>

  Senza SESSION, il comportamento e' identico al coordinator base.

READ REPAIR:
  Quando il coordinator legge da piu' repliche e trova versioni
  diverse, dopo aver risposto al client, aggiorna le repliche
  stale con la versione piu' recente (in background).

GARBAGE COLLECTION:
  Le sessioni inattive vengono rimosse dopo session_gc_timeout secondi
  (default: 300s = 5 minuti) da un thread in background.
"""

import argparse
import json
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable


# Tipo per gli handler dei comandi: riceve argomenti, restituisce (risposta, should_close)
CommandHandler = Callable[[str], tuple[str, bool]]

# Timeout in secondi per garbage collection delle sessioni inattive.
SESSION_GC_TIMEOUT = 300.0


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
    --host/--port:          indirizzo del coordinator
    --read-quorum:          numero minimo di repliche da leggere (R)
    --write-quorum:         numero minimo di ACK per considerare la write riuscita (W)
    --replicas:             lista di endpoint replica nel formato host:port
    --session-gc-timeout:   secondi di inattivita' prima di eliminare una sessione
    """
    parser = argparse.ArgumentParser(description="Quorum coordinator stateful con session consistency")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6420)
    parser.add_argument("--read-quorum", type=int, default=2)
    parser.add_argument("--write-quorum", type=int, default=2)
    parser.add_argument("--session-gc-timeout", type=float, default=SESSION_GC_TIMEOUT,
                        help="Secondi di inattivita' prima di eliminare una sessione")
    parser.add_argument(
        "--replicas", nargs="+",
        default=["127.0.0.1:6421", "127.0.0.1:6422", "127.0.0.1:6423"],
    )
    return parser.parse_args()


# =====================================================================
# Stato di sessione per un singolo client
# =====================================================================

class SessionState:
    """
    Traccia la versione massima osservata per ogni chiave da un client.

    Quando un client con SESSION id legge o scrive una chiave, il
    coordinator aggiorna questa struttura. Le letture successive
    verificano che la versione restituita sia >= a quella registrata.
    """

    def __init__(self) -> None:
        self.last_seen: dict[str, int] = {}  # key -> max version seen
        self.last_access: float = time.monotonic()

    def update(self, key: str, version: int) -> None:
        """Aggiorna la versione minima osservata per una chiave."""
        current = self.last_seen.get(key, -1)
        if version > current:
            self.last_seen[key] = version
        self.last_access = time.monotonic()

    def min_version_for(self, key: str) -> int:
        """Restituisce la versione minima accettabile per una chiave, o -1 se nessuna."""
        self.last_access = time.monotonic()
        return self.last_seen.get(key, -1)


# =====================================================================
# Coordinator stateful
# =====================================================================

class StatefulQuorumCoordinator:
    """
    Coordinator con quorum, session consistency stateful e read repair.

    Responsabilita':
    - Ricevere comandi testuali dai client
    - Tradurli in RPC JSON verso le repliche
    - Applicare la logica di quorum (R letture, W scritture)
    - Mantenere lo stato di sessione per client (SESSION)
    - Verificare il vincolo di sessione sulle letture
    - Eseguire read repair sulle repliche stale
    - Garbage collection delle sessioni inattive
    """

    def __init__(
        self,
        replicas: list[ReplicaEndpoint],
        read_quorum: int,
        write_quorum: int,
        session_gc_timeout: float = SESSION_GC_TIMEOUT,
    ) -> None:
        self._replicas = replicas
        self._read_quorum = read_quorum
        self._write_quorum = write_quorum
        self._session_gc_timeout = session_gc_timeout

        # Sessioni: client_id -> SessionState
        self._sessions: dict[str, SessionState] = {}
        self._sessions_lock = threading.Lock()

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

        # Avvia thread di garbage collection sessioni
        gc_thread = threading.Thread(target=self._gc_sessions_loop, daemon=True)
        gc_thread.start()

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
    # Gestione sessioni
    # =====================================================================

    def _get_session(self, client_id: str) -> SessionState:
        """Restituisce la sessione per un client, creandola se non esiste."""
        with self._sessions_lock:
            session = self._sessions.get(client_id)
            if session is None:
                session = SessionState()
                self._sessions[client_id] = session
                log(f"session created for client {client_id}")
            return session

    def _gc_sessions_loop(self) -> None:
        """Periodicamente rimuove sessioni inattive."""
        while True:
            time.sleep(60.0)
            now = time.monotonic()
            with self._sessions_lock:
                expired = [
                    cid for cid, session in self._sessions.items()
                    if (now - session.last_access) > self._session_gc_timeout
                ]
                for cid in expired:
                    del self._sessions[cid]
                    log(f"session gc: removed session for client {cid}")

    # =====================================================================
    # Parsing parametro SESSION
    # =====================================================================

    def _extract_session(self, tokens: list[str]) -> tuple[list[str], str | None]:
        """
        Cerca SESSION client_id in coda ai tokens.

        Restituisce (tokens_senza_session, client_id_o_None).
        Esempio: ["key", "value", "SESSION", "clientA"] -> (["key", "value"], "clientA")
        """
        if len(tokens) >= 2 and tokens[-2].upper() == "SESSION":
            client_id = tokens[-1]
            return tokens[:-2], client_id
        return tokens, None

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
    # Lettura session-aware (SESSION + fallback + read repair)
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
        """STATUS -> OK N=<n> R=<r> W=<w> sessions=<s>. Info sul cluster."""
        if argument_blob.strip():
            return "ERR usage: STATUS", False
        with self._sessions_lock:
            active_sessions = len(self._sessions)
        return (
            f"OK N={len(self._replicas)} R={self._read_quorum} "
            f"W={self._write_quorum} sessions={active_sessions}",
            False,
        )

    def _handle_set(self, argument_blob: str) -> tuple[str, bool]:
        """
        SET <key> <value> [SESSION <client_id>]

        1. Parsa la chiave, il valore, e l'eventuale SESSION
        2. Legge la versione corrente da TUTTE le repliche
        3. Calcola next_version = max_version + 1
        4. Scrive su tutte le repliche con next_version
        5. Se almeno W repliche confermano (ACK):
           - aggiorna la sessione (se presente)
           - restituisce OK
        """
        tokens = argument_blob.split()
        if len(tokens) < 2:
            return "ERR usage: SET <key> <value> [SESSION <client_id>]", False

        tokens, client_id = self._extract_session(tokens)
        if len(tokens) < 2:
            return "ERR usage: SET <key> <value> [SESSION <client_id>]", False

        key = tokens[0]
        value = " ".join(tokens[1:])

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
                    # Aggiorna la sessione
                    if client_id is not None:
                        session = self._get_session(client_id)
                        session.update(key, next_version)
                    return (
                        f"OK version={next_version} acks={acknowledgements}",
                        False,
                    )

        return f"ERR write quorum not reached acks={acknowledgements}", False

    def _handle_getv(self, argument_blob: str) -> tuple[str, bool]:
        """
        GETV <key> [SESSION <client_id>]

        Lettura con session consistency e read repair.
        Se SESSION e' specificato, il coordinator usa la sessione per
        determinare la versione minima accettabile.
        """
        tokens = argument_blob.strip().split()
        if not tokens:
            return "ERR usage: GETV <key> [SESSION <client_id>]", False

        tokens, client_id = self._extract_session(tokens)
        if len(tokens) != 1:
            return "ERR usage: GETV <key> [SESSION <client_id>]", False

        key = tokens[0]

        # Determina la versione minima dalla sessione
        min_version: int | None = None
        session: SessionState | None = None
        if client_id is not None:
            session = self._get_session(client_id)
            sv = session.min_version_for(key)
            if sv >= 0:
                min_version = sv

        # Lettura session-aware
        version, value, reads = self._session_aware_read(key, min_version)

        if len(reads) < self._read_quorum:
            return f"ERR read quorum not reached responses={len(reads)}", False

        if version < 0 or value is None:
            return "NOT_FOUND", False

        if min_version is not None and version < min_version:
            return (
                f"ERR stale session_min={min_version} best={version}",
                False,
            )

        # Aggiorna la sessione con la versione letta
        if session is not None:
            session.update(key, version)

        return f"OK {value} version={version}", False

    def _handle_get(self, argument_blob: str) -> tuple[str, bool]:
        """
        GET <key> [SESSION <client_id>]

        Come GETV ma senza version= nella risposta di successo.
        """
        tokens = argument_blob.strip().split()
        if not tokens:
            return "ERR usage: GET <key> [SESSION <client_id>]", False

        tokens, client_id = self._extract_session(tokens)
        if len(tokens) != 1:
            return "ERR usage: GET <key> [SESSION <client_id>]", False

        key = tokens[0]

        min_version: int | None = None
        session: SessionState | None = None
        if client_id is not None:
            session = self._get_session(client_id)
            sv = session.min_version_for(key)
            if sv >= 0:
                min_version = sv

        version, value, reads = self._session_aware_read(key, min_version)

        if len(reads) < self._read_quorum:
            return f"ERR read quorum not reached responses={len(reads)}", False

        if version < 0 or value is None:
            return "NOT_FOUND", False

        if min_version is not None and version < min_version:
            return (
                f"ERR stale session_min={min_version} best={version}",
                False,
            )

        if session is not None:
            session.update(key, version)

        return f"OK {value}", False

    def _handle_cas(self, argument_blob: str) -> tuple[str, bool]:
        """
        CAS <key> <expected_version> <new_value> [SESSION <client_id>]

        Compare-And-Swap:
        1. Parsa chiave, expected_version, valore, eventuale SESSION
        2. Controlla che expected_version non sia inferiore alla sessione
        3. Legge da tutte le repliche per trovare la versione corrente
        4. Verifica che current_version == expected_version
        5. Scrive new_value con version = expected + 1
        6. Aggiorna la sessione

        Usa "type": "write" verso le repliche (la replica accetta
        solo se incoming_version >= current_version).
        """
        tokens = argument_blob.strip().split()
        if len(tokens) < 3:
            return "ERR usage: CAS <key> <expected_version> <new_value> [SESSION <client_id>]", False

        tokens, client_id = self._extract_session(tokens)
        if len(tokens) < 3:
            return "ERR usage: CAS <key> <expected_version> <new_value> [SESSION <client_id>]", False

        key = tokens[0]
        try:
            expected_version = int(tokens[1])
        except ValueError:
            return "ERR expected_version must be an integer", False
        new_value = " ".join(tokens[2:])

        # Controlla vincolo di sessione sulla expected_version
        if client_id is not None:
            session = self._get_session(client_id)
            sv = session.min_version_for(key)
            if sv >= 0 and expected_version < sv:
                return (
                    f"ERR session_version_conflict expected={expected_version} session_min={sv}",
                    False,
                )

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
                    # Aggiorna la sessione
                    if client_id is not None:
                        session = self._get_session(client_id)
                        session.update(key, new_version)
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
    coordinator: StatefulQuorumCoordinator,
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
    Entry point del coordinator stateful.
    1. Parsa argomenti
    2. Costruisce la lista di ReplicaEndpoint
    3. Crea il StatefulQuorumCoordinator
    4. Apre socket TCP e accetta client in thread separati
    """
    args = parse_args()
    replicas = [
        ReplicaEndpoint(
            host=entry.split(":", 1)[0], port=int(entry.split(":", 1)[1])
        )
        for entry in args.replicas
    ]
    coordinator = StatefulQuorumCoordinator(
        replicas, args.read_quorum, args.write_quorum, args.session_gc_timeout
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((args.host, args.port))
        server_socket.listen()
        log(
            f"stateful session coordinator listening on {args.host}:{args.port} "
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
