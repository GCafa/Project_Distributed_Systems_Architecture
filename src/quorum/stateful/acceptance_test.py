"""
Test di accettazione per Session Consistency STATEFUL (Homework 5).

Questo script:
1. Avvia 3 repliche e 1 coordinator_stateful automaticamente
2. Esegue 9 test che coprono i casi nominali e critici
3. Spegne tutto alla fine

I test verificano:
  TS1 - SET + GETV base con SESSION
  TS2 - CAS successo e fallimento con SESSION
  TS3 - Read-your-writes via SESSION (automatico, senza MIN_VERSION)
  TS4 - Monotonic reads via SESSION
  TS5 - Senza SESSION -> nessuna protezione (comportamento base)
  TS6 - Read repair (convergenza dopo lettura)
  TS7 - Quorum non raggiungibile
  TS8 - CAS + session consistency
  TS9 - Sessioni indipendenti (clientA e clientB non interferiscono)

COME ESEGUIRE:
  python acceptance_test.py

REQUISITI:
  - Python 3.10+
  - Nessuna dipendenza esterna
"""

import json
import socket
import subprocess
import sys
import time
from pathlib import Path

# === CONFIGURAZIONE ===
HOST = "127.0.0.1"
COORDINATOR_PORT = 6430
REQUEST_TIMEOUT = 10.0
REPLICAS = [
    ("R0", 6431),
    ("R1", 6432),
    ("R2", 6433),
]

# Contatori per il report finale
passed = 0
failed = 0


def wait_for_port(port: int, timeout: float = 5.0) -> None:
    """
    Attende che un server TCP sia raggiungibile sulla porta specificata.
    Utile dopo aver avviato un sottoprocesso per assicurarsi che sia pronto.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} did not open within {timeout}s")


def request(command: str, port: int = COORDINATOR_PORT) -> str:
    """
    Invia un singolo comando testuale al coordinator e restituisce la risposta.
    Ogni chiamata apre una nuova connessione TCP (stateless).
    """
    with socket.create_connection((HOST, port), timeout=REQUEST_TIMEOUT) as connection:
        connection_file = connection.makefile("rwb")
        connection_file.write((command + "\n").encode("utf-8"))
        connection_file.flush()
        return connection_file.readline().decode("utf-8", errors="replace").strip()


def request_to(command: str, port: int) -> str:
    """Invia un comando a un coordinator su una porta specifica."""
    return request(command, port=port)


def rpc_to_replica(port: int, message: dict) -> dict | None:
    """
    Invia un messaggio JSON direttamente a una replica (bypassando il coordinator).
    Usato nei test per manipolare lo stato delle repliche e simulare guasti.
    """
    try:
        with socket.create_connection((HOST, port), timeout=1.0) as connection:
            connection_file = connection.makefile("rwb")
            payload = json.dumps(message) + "\n"
            connection_file.write(payload.encode("utf-8"))
            connection_file.flush()
            resp = connection_file.readline().decode("utf-8", errors="replace").strip()
            return json.loads(resp)
    except (OSError, json.JSONDecodeError):
        return None


def expect(command: str, prefix: str, test_name: str = "") -> str:
    """
    Invia un comando e verifica che la risposta inizi con il prefisso atteso.
    Stampa il risultato e aggiorna i contatori passed/failed.
    """
    global passed, failed
    response = request(command)
    label = f"[{test_name}] " if test_name else ""
    ok = response.startswith(prefix)
    status = "PASS" if ok else "FAIL"
    print(f"  {label}{command} -> {response} [{status}]")
    if ok:
        passed += 1
    else:
        failed += 1
        print(f"    EXPECTED prefix: {prefix!r}")
    return response


def expect_not(command: str, prefix: str, test_name: str = "") -> str:
    """
    Verifica che la risposta NON inizi con il prefisso specificato.
    Usato per test negativi (es. verificare che un errore venga restituito).
    """
    global passed, failed
    response = request(command)
    label = f"[{test_name}] " if test_name else ""
    ok = not response.startswith(prefix)
    status = "PASS" if ok else "FAIL"
    print(f"  {label}{command} -> {response} [{status}]")
    if ok:
        passed += 1
    else:
        failed += 1
        print(f"    EXPECTED NOT to start with: {prefix!r}")
    return response


def main() -> None:
    global passed, failed
    root = Path(__file__).resolve().parent
    processes: list[subprocess.Popen] = []

    try:
        # =================================================================
        # SETUP: avvia 3 repliche e 1 coordinator_stateful
        # =================================================================
        print("=" * 60)
        print("SETUP: Starting replicas and stateful coordinator...")
        print("=" * 60)

        for node_id, port in REPLICAS:
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        str(root / "replica_node.py"),
                        "--node-id", node_id,
                        "--port", str(port),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )
            wait_for_port(port)
            print(f"  Replica {node_id} started on port {port}")

        replica_args = [f"{HOST}:{port}" for _, port in REPLICAS]
        processes.append(
            subprocess.Popen(
                [
                    sys.executable,
                        str(root / "coordinator.py"),
                    "--port", str(COORDINATOR_PORT),
                    "--read-quorum", "2",
                    "--write-quorum", "2",
                    "--replicas", *replica_args,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        wait_for_port(COORDINATOR_PORT)
        print(f"  Stateful coordinator started on port {COORDINATOR_PORT}")
        print()

        # =================================================================
        # TS1: SET + GETV base con SESSION
        # Il coordinator traccia le versioni per clientA.
        # =================================================================
        print("-" * 60)
        print("TS1: SET + GETV base con SESSION")
        print("-" * 60)
        expect("PING", "OK PONG", "TS1")
        expect("SET alpha one SESSION clientA", "OK version=0", "TS1")
        expect("GETV alpha SESSION clientA", "OK one version=0", "TS1")
        expect("SET beta two SESSION clientA", "OK version=0", "TS1")
        expect("GETV beta SESSION clientA", "OK two version=0", "TS1")
        print()

        # =================================================================
        # TS2: CAS successo e fallimento con SESSION
        # =================================================================
        print("-" * 60)
        print("TS2: CAS successo e fallimento con SESSION")
        print("-" * 60)
        expect("CAS alpha 0 updated_one SESSION clientA", "OK version=1", "TS2")
        expect("GETV alpha SESSION clientA", "OK updated_one version=1", "TS2")
        # CAS sotto la versione minima della sessione -> session_version_conflict
        expect("CAS alpha 0 stale_write SESSION clientA", "ERR session_version_conflict", "TS2")
        # Il valore non deve essere cambiato
        expect("GETV alpha SESSION clientA", "OK updated_one version=1", "TS2")
        print()

        # =================================================================
        # TS3: Read-your-writes via SESSION (automatico)
        # A differenza del test stateless, qui il client NON deve
        # specificare MIN_VERSION: il coordinator lo fa internamente
        # grazie alla sessione memorizzata lato server.
        # =================================================================
        print("-" * 60)
        print("TS3: Read-your-writes (session automatica)")
        print("-" * 60)
        expect("SET gamma hello SESSION clientA", "OK version=0", "TS3")
        # Dopo SET v=0, GETV con stessa sessione deve restituire >= v=0
        expect("GETV gamma SESSION clientA", "OK hello version=0", "TS3")
        # Scrivo una nuova versione
        expect("SET gamma world SESSION clientA", "OK version=1", "TS3")
        # Dopo SET v=1, la sessione richiede automaticamente >= v=1
        expect("GETV gamma SESSION clientA", "OK world version=1", "TS3")
        print()

        # =================================================================
        # TS4: Monotonic reads via SESSION
        # Dopo aver letto una versione alta, le letture successive
        # non possono restituire versioni inferiori.
        # =================================================================
        print("-" * 60)
        print("TS4: Monotonic reads via SESSION")
        print("-" * 60)
        expect("SET delta v1 SESSION clientA", "OK version=0", "TS4")
        expect("SET delta v2 SESSION clientA", "OK version=1", "TS4")
        # Lettura restituisce la versione piu' recente
        expect("GETV delta SESSION clientA", "OK v2 version=1", "TS4")
        # Letture successive garantite >= 1 dalla sessione
        expect("GETV delta SESSION clientA", "OK v2 version=1", "TS4")
        print()

        # =================================================================
        # TS5: Senza SESSION -> nessuna protezione
        # Dimostra che senza il parametro SESSION, il coordinator
        # stateful si comporta come il coordinator base, senza alcuna
        # garanzia di sessione.
        # =================================================================
        print("-" * 60)
        print("TS5: Senza SESSION (nessuna protezione)")
        print("-" * 60)
        expect("SET epsilon first", "OK version=0", "TS5")
        # Senza SESSION, il GETV funziona ma senza vincoli di sessione
        expect("GETV epsilon", "OK first version=0", "TS5")
        expect("SET epsilon second", "OK version=1", "TS5")
        expect("GETV epsilon", "OK second version=1", "TS5")
        print()

        # =================================================================
        # TS6: Read repair
        # Scrivo direttamente su R0 un valore che le altre repliche non
        # hanno. Dopo una GETV (che interroga tutte le repliche e fa
        # read repair), le repliche stale dovrebbero convergere.
        # =================================================================
        print("-" * 60)
        print("TS6: Read repair")
        print("-" * 60)
        # Scrivo "zeta" via coordinator (tutte le repliche lo ricevono)
        expect("SET zeta original SESSION clientA", "OK version=0", "TS6")

        # Ora scrivo direttamente su R0 una versione piu' nuova
        rpc_to_replica(REPLICAS[0][1], {
            "type": "write", "key": "zeta", "value": "updated_on_r0", "version": 5,
        })

        # GETV dovrebbe restituire la versione piu' alta (5) grazie al quorum
        # e poi fare read repair sulle altre repliche
        expect("GETV zeta SESSION clientA", "OK updated_on_r0 version=5", "TS6")

        # Aspetto un attimo che il read repair si propaghi
        time.sleep(0.5)

        # Verifico che R1 abbia ricevuto il read repair
        r1_resp = rpc_to_replica(REPLICAS[1][1], {"type": "read", "key": "zeta"})
        if r1_resp and r1_resp.get("version") == 5:
            print(f"  [TS6] Read repair verified on R1: version={r1_resp['version']} [PASS]")
            passed += 1
        else:
            print(f"  [TS6] Read repair on R1: {r1_resp} [FAIL]")
            failed += 1
        print()

        # =================================================================
        # TS7: Quorum non raggiungibile
        # Avviamo un coordinator separato con repliche inesistenti per
        # verificare che il quorum failure viene segnalato correttamente.
        # =================================================================
        print("-" * 60)
        print("TS7: Quorum non raggiungibile")
        print("-" * 60)

        # Caso nominale: il coordinator principale funziona
        # STATUS nel coordinator stateful include sessions=<n>
        resp = request("STATUS")
        if resp.startswith("OK N=3 R=2 W=2 sessions="):
            print(f"  [TS7] STATUS -> {resp} [PASS]")
            passed += 1
        else:
            print(f"  [TS7] STATUS -> {resp} [FAIL]")
            print(f"    EXPECTED prefix: 'OK N=3 R=2 W=2 sessions='")
            failed += 1

        expect("GETV nonexistent_key SESSION clientA", "NOT_FOUND", "TS7")

        # Caso critico: coordinator con repliche irraggiungibili
        unreachable_coord_port = 6462
        unreachable_coord = subprocess.Popen(
            [
                sys.executable,
                str(root / "coordinator.py"),
                "--port", str(unreachable_coord_port),
                "--read-quorum", "2",
                "--write-quorum", "2",
                "--replicas", "127.0.0.1:9990", "127.0.0.1:9991", "127.0.0.1:9992",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(unreachable_coord)
        wait_for_port(unreachable_coord_port)

        # Lettura -> ERR read quorum not reached
        read_resp = request_to("GETV any_key SESSION clientA", unreachable_coord_port)
        print(f"  [TS7] GETV (repliche irraggiungibili) -> {read_resp}")
        if read_resp.startswith("ERR read quorum not reached"):
            print("  [TS7] Read quorum failure correttamente segnalato [PASS]")
            passed += 1
        else:
            print(f"  [TS7] Expected 'ERR read quorum not reached' [FAIL]")
            failed += 1

        # Scrittura -> ERR write quorum not reached
        write_resp = request_to("SET any_key any_value SESSION clientA", unreachable_coord_port)
        print(f"  [TS7] SET (repliche irraggiungibili) -> {write_resp}")
        if write_resp.startswith("ERR write quorum not reached"):
            print("  [TS7] Write quorum failure correttamente segnalato [PASS]")
            passed += 1
        else:
            print(f"  [TS7] Expected 'ERR write quorum not reached' [FAIL]")
            failed += 1

        print()

        # =================================================================
        # TS8: CAS + session consistency
        # Dopo un CAS riuscito, la sessione del coordinator deve avanzare
        # e proteggere letture successive. Il client non deve fare nulla
        # di esplicito: la protezione e' automatica.
        # =================================================================
        print("-" * 60)
        print("TS8: CAS + session consistency")
        print("-" * 60)
        expect("SET eta start SESSION clientA", "OK version=0", "TS8")
        expect("CAS eta 0 middle SESSION clientA", "OK version=1", "TS8")
        # Dopo CAS a version=1, la sessione protegge automaticamente
        expect("GETV eta SESSION clientA", "OK middle version=1", "TS8")
        expect("CAS eta 1 final SESSION clientA", "OK version=2", "TS8")
        # Dopo CAS a version=2, la sessione richiede >= 2
        expect("GETV eta SESSION clientA", "OK final version=2", "TS8")

        # CAS con expected_version inferiore alla sessione -> ERR session_version_conflict
        # La sessione ha eta=2, quindi expected_version=0 e' inaccettabile
        expect("CAS eta 0 should_fail SESSION clientA", "ERR session_version_conflict", "TS8")
        print()

        # =================================================================
        # TS9: Sessioni indipendenti (clientA e clientB)
        # Due client con SESSION diverse non devono interferire tra loro.
        # Ogni sessione traccia le proprie versioni indipendentemente.
        # =================================================================
        print("-" * 60)
        print("TS9: Sessioni indipendenti (clientA vs clientB)")
        print("-" * 60)

        # clientA scrive e legge
        expect("SET iota A_value SESSION clientA", "OK version=0", "TS9")
        expect("GETV iota SESSION clientA", "OK A_value version=0", "TS9")

        # clientB non ha mai interagito con iota -> la sua sessione non
        # ha vincoli su questa chiave, ma la lettura funziona comunque
        expect("GETV iota SESSION clientB", "OK A_value version=0", "TS9")

        # clientB scrive una chiave diversa
        expect("SET kappa B_value SESSION clientB", "OK version=0", "TS9")
        expect("GETV kappa SESSION clientB", "OK B_value version=0", "TS9")

        # clientA non ha vincoli di sessione su kappa
        expect("GETV kappa SESSION clientA", "OK B_value version=0", "TS9")

        # Verifica che le sessioni siano effettivamente separate:
        # clientA aggiorna iota a v=1, clientB legge iota senza vincolo di sessione
        expect("SET iota A_updated SESSION clientA", "OK version=1", "TS9")
        # clientA ha sessione iota=1, la sua lettura e' protetta
        expect("GETV iota SESSION clientA", "OK A_updated version=1", "TS9")
        # clientB ha sessione iota=0 (dalla lettura precedente), quindi
        # anche la sua lettura e' protetta con min_version=0, che e' soddisfatto
        expect("GETV iota SESSION clientB", "OK A_updated version=1", "TS9")

        # Verifico il conteggio sessioni nel STATUS
        status_resp = request("STATUS")
        print(f"  [TS9] STATUS -> {status_resp}")
        if "sessions=" in status_resp:
            sessions_str = status_resp.split("sessions=")[1].split()[0]
            sessions_count = int(sessions_str)
            if sessions_count >= 2:
                print(f"  [TS9] Active sessions >= 2 (clientA + clientB) [PASS]")
                passed += 1
            else:
                print(f"  [TS9] Expected at least 2 sessions, got {sessions_count} [FAIL]")
                failed += 1
        else:
            print(f"  [TS9] Expected 'sessions=' in STATUS response [FAIL]")
            failed += 1

        print()

        # =================================================================
        # REPORT
        # =================================================================
        print("=" * 60)
        total = passed + failed
        print(f"RESULTS: {passed}/{total} passed, {failed}/{total} failed")
        if failed == 0:
            print("ALL TESTS PASSED!")
        else:
            print("SOME TESTS FAILED!")
        print("=" * 60)

    finally:
        # Shutdown di tutti i processi
        for process in reversed(processes):
            process.terminate()
        for process in reversed(processes):
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2.0)


if __name__ == "__main__":
    main()
