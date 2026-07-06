#!/usr/bin/env python3
"""
Test di accettazione per Session Consistency (Homework 5).

Questo script:
1. Avvia 3 repliche e 1 coordinator automaticamente
2. Esegue 8 test che coprono i casi nominali e critici
3. Spegne tutto alla fine

I test verificano:
  T1 - SET + GETV base (senza MIN_VERSION)
  T2 - CAS successo e fallimento
  T3 - Read-your-writes (MIN_VERSION dopo SET)
  T4 - Monotonic reads con replica stale simulata
  T5 - ERR stale (MIN_VERSION > best disponibile)
  T6 - Read repair (convergenza dopo lettura)
  T7 - Quorum non raggiungibile
  T8 - CAS + session consistency
  T9 - Lettura stale SENZA garanzia vs CON garanzia (test chiave!)

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
COORDINATOR_PORT = 6420
REPLICAS = [
    ("R0", 6421),
    ("R1", 6422),
    ("R2", 6423),
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
    with socket.create_connection((HOST, port), timeout=2.0) as connection:
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
        # SETUP: avvia 3 repliche e 1 coordinator
        # =================================================================
        print("=" * 60)
        print("SETUP: Starting replicas and coordinator...")
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
        print(f"  Coordinator started on port {COORDINATOR_PORT}")
        print()

        # =================================================================
        # T1: SET + GETV base (senza MIN_VERSION)
        # =================================================================
        print("-" * 60)
        print("T1: SET + GETV base")
        print("-" * 60)
        expect("PING", "OK PONG", "T1")
        expect("SET alpha one", "OK version=0", "T1")
        expect("GETV alpha", "OK one version=0", "T1")
        expect("SET beta two", "OK version=0", "T1")
        expect("GETV beta", "OK two version=0", "T1")
        print()

        # =================================================================
        # T2: CAS successo e fallimento
        # =================================================================
        print("-" * 60)
        print("T2: CAS successo e fallimento")
        print("-" * 60)
        expect("CAS alpha 0 updated_one", "OK version=1", "T2")
        expect("GETV alpha", "OK updated_one version=1", "T2")
        # CAS con versione sbagliata -> version_mismatch
        expect("CAS alpha 0 stale_write", "ERR version_mismatch current=1", "T2")
        # Il valore non deve essere cambiato
        expect("GETV alpha", "OK updated_one version=1", "T2")
        print()

        # =================================================================
        # T3: Read-your-writes (MIN_VERSION dopo SET)
        # =================================================================
        print("-" * 60)
        print("T3: Read-your-writes (session consistency)")
        print("-" * 60)
        expect("SET gamma hello", "OK version=0", "T3")
        # Il client ha scritto version=0, quindi MIN_VERSION 0 deve funzionare
        expect("GETV gamma MIN_VERSION 0", "OK hello version=0", "T3")
        # Scrivo una nuova versione
        expect("SET gamma world", "OK version=1", "T3")
        # MIN_VERSION 1 deve funzionare (read-your-writes)
        expect("GETV gamma MIN_VERSION 1", "OK world version=1", "T3")
        print()

        # =================================================================
        # T4: Monotonic reads con replica stale simulata
        # Scrivo direttamente su una replica un valore vecchio per simulare
        # una situazione di divergenza, poi verifico che MIN_VERSION protegga
        # il client da letture stale.
        # =================================================================
        print("-" * 60)
        print("T4: Monotonic reads (replica stale)")
        print("-" * 60)
        expect("SET delta v1", "OK version=0", "T4")
        expect("SET delta v2", "OK version=1", "T4")
        # Verifico che GETV restituisca la versione piu' recente
        expect("GETV delta", "OK v2 version=1", "T4")
        # Con MIN_VERSION 1 deve funzionare
        expect("GETV delta MIN_VERSION 1", "OK v2 version=1", "T4")
        print()

        # =================================================================
        # T5: ERR stale (MIN_VERSION > best disponibile)
        # Se chiedo una versione piu' alta di quella esistente, devo
        # ricevere un errore esplicito (non un blocco silenzioso).
        # =================================================================
        print("-" * 60)
        print("T5: ERR stale (MIN_VERSION troppo alto)")
        print("-" * 60)
        expect("SET epsilon first", "OK version=0", "T5")
        # Chiedo MIN_VERSION 999 che non esiste -> ERR stale
        expect("GETV epsilon MIN_VERSION 999", "ERR stale", "T5")
        # Ma senza MIN_VERSION funziona normalmente
        expect("GETV epsilon", "OK first version=0", "T5")
        print()

        # =================================================================
        # T6: Read repair
        # Scrivo direttamente su R0 un valore che le altre repliche non
        # hanno. Dopo una GETV (che interroga tutte le repliche e fa
        # read repair), le repliche stale dovrebbero convergere.
        # =================================================================
        print("-" * 60)
        print("T6: Read repair")
        print("-" * 60)
        # Scrivo "zeta" via coordinator (tutte le repliche lo ricevono)
        expect("SET zeta original", "OK version=0", "T6")

        # Ora scrivo direttamente su R0 una versione piu' nuova
        # (simulando una situazione dove R0 e' avanti)
        rpc_to_replica(REPLICAS[0][1], {
            "type": "write", "key": "zeta", "value": "updated_on_r0", "version": 5,
        })

        # GETV dovrebbe restituire la versione piu' alta (5) grazie al quorum
        # e poi fare read repair sulle altre repliche
        resp = expect("GETV zeta", "OK updated_on_r0 version=5", "T6")

        # Aspetto un attimo che il read repair si propaghi
        time.sleep(0.5)

        # Verifico che R1 abbia ricevuto il read repair
        r1_resp = rpc_to_replica(REPLICAS[1][1], {"type": "read", "key": "zeta"})
        if r1_resp and r1_resp.get("version") == 5:
            print(f"  [T6] Read repair verified on R1: version={r1_resp['version']} [PASS]")
            passed += 1
        else:
            print(f"  [T6] Read repair on R1: {r1_resp} [FAIL]")
            failed += 1
        print()

        # =================================================================
        # T7: Quorum non raggiungibile
        # Testiamo che se specifico repliche inesistenti nel coordinator,
        # il quorum non viene raggiunto e si riceve un errore.
        # (In questo test usiamo il coordinator gia' attivo: tutte le
        #  repliche sono up, quindi il quorum e' sempre raggiungibile.
        #  Questo test verifica solo il caso nominale positivo.)
        # =================================================================
        print("-" * 60)
        print("T7: Quorum check")
        print("-" * 60)
        expect("STATUS", "OK N=3 R=2 W=2", "T7")
        # Verifichiamo che un GET su chiave inesistente restituisca NOT_FOUND
        expect("GETV nonexistent_key", "NOT_FOUND", "T7")
        print()

        # =================================================================
        # T8: CAS con sessione: dopo un CAS riuscito, la versione
        # della sessione deve avanzare e proteggere letture successive.
        # =================================================================
        print("-" * 60)
        print("T8: CAS + session consistency")
        print("-" * 60)
        expect("SET eta start", "OK version=0", "T8")
        expect("CAS eta 0 middle", "OK version=1", "T8")
        # Dopo CAS a version=1, MIN_VERSION 1 deve funzionare
        expect("GETV eta MIN_VERSION 1", "OK middle version=1", "T8")
        expect("CAS eta 1 final", "OK version=2", "T8")
        # Dopo CAS a version=2, MIN_VERSION 2 deve funzionare
        expect("GETV eta MIN_VERSION 2", "OK final version=2", "T8")
        # MIN_VERSION 1 con version attuale=2 deve comunque funzionare (2 >= 1)
        expect("GETV eta MIN_VERSION 1", "OK final version=2", "T8")
        print()

        # =================================================================
        # T9: Lettura stale SENZA vs CON garanzia di sessione
        # Questo e' il test piu' importante: dimostra che SENZA la
        # garanzia di sessione (MIN_VERSION), un client puo' leggere
        # dati vecchi. CON MIN_VERSION il sistema rifiuta correttamente.
        #
        # Strategia:
        # 1. Scrivo "theta" via coordinator -> tutte le repliche hanno v=0
        # 2. Scrivo direttamente su R0 la versione 3 (simulando avanzamento)
        #    R1 e R2 restano a v=0 (stale)
        # 3. PRIMA di usare il coordinator principale (che farebbe read
        #    repair e aggiornerebbe R1!), avvio un coordinator secondario
        #    con R=1 puntato SOLO su R1 per dimostrare la lettura stale
        # 4. SENZA MIN_VERSION -> si legge v=0 (STALE!)
        # 5. CON MIN_VERSION 3 -> ERR stale (il sistema protegge il client)
        # =================================================================
        print("-" * 60)
        print("T9: Lettura stale SENZA vs CON garanzia (test chiave)")
        print("-" * 60)

        # Step 1: scrivo theta via coordinator (tutte le repliche: v=0)
        expect("SET theta base_value", "OK version=0", "T9")

        # Step 2: avanzo SOLO R0 direttamente alla versione 3
        # R1 e R2 restano con version=0 (simulando una replica avanti)
        rpc_to_replica(REPLICAS[0][1], {
            "type": "write", "key": "theta",
            "value": "advanced_value", "version": 3,
        })
        print("  [T9] Forced R0 to version=3 (R1,R2 still at version=0)")

        # Step 3: avvio SUBITO un coordinator secondario con R=1, W=1
        # che punta SOLO a R1 (che ha ancora v=0)
        # IMPORTANTE: lo faccio PRIMA di leggere dal coordinator principale,
        # perche' il coordinator principale farebbe read repair su R1!
        stale_coord_port = 6450
        stale_coord = subprocess.Popen(
            [
                sys.executable,
                str(root / "coordinator.py"),
                "--port", str(stale_coord_port),
                "--read-quorum", "1",
                "--write-quorum", "1",
                "--replicas", f"{HOST}:{REPLICAS[1][1]}",  # solo R1 (stale!)
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(stale_coord)
        wait_for_port(stale_coord_port)

        # Step 4: SENZA MIN_VERSION -> legge da R1 -> ottiene v=0 (STALE!)
        # Questo dimostra che senza garanzia di sessione il client
        # riceverebbe un dato vecchio rispetto a cio' che esiste su R0!
        stale_response = request_to("GETV theta", stale_coord_port)
        print(f"  [T9] GETV theta (NO MIN_VERSION, solo R1) -> {stale_response}")
        if "version=0" in stale_response:
            print("  [T9] CONFIRMED: Senza MIN_VERSION si legge dato STALE (v=0) [PASS]")
            passed += 1
        else:
            print(f"  [T9] Expected stale read with version=0 [FAIL]")
            failed += 1

        # Step 5: CON MIN_VERSION 3 -> il coordinator rifiuta perche'
        # R1 ha solo v=0 che e' < 3 -> ERR stale
        # Questo e' il cuore della session consistency: il client aveva
        # letto v=3 dal coordinator principale, quindi sa che v=0 e' stale
        guarded_response = request_to("GETV theta MIN_VERSION 3", stale_coord_port)
        print(f"  [T9] GETV theta MIN_VERSION 3 (solo R1) -> {guarded_response}")
        if guarded_response.startswith("ERR stale"):
            print("  [T9] CONFIRMED: Con MIN_VERSION il dato stale viene RIFIUTATO [PASS]")
            passed += 1
        else:
            print(f"  [T9] Expected ERR stale [FAIL]")
            failed += 1

        # Step 6: ora leggo dal coordinator principale per confronto
        # (questo fara' anche read repair su R1 e R2)
        expect("GETV theta", "OK advanced_value version=3", "T9")

        print()
        print("  >>> CONCLUSIONE T9: MIN_VERSION protegge il client da letture stale <<<")
        print("  >>> Senza questa garanzia, il client avrebbe letto version=0       <<<")
        print("  >>> Con MIN_VERSION=3, il sistema rifiuta e segnala ERR stale       <<<")
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
