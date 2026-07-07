"""
Test di accettazione per KV store con replica primary-secondary.

Avvia un secondario, un primary asincrono e un primary sincrono, poi verifica:
- comandi base sul primary;
- secondario read-only;
- convergenza della replica asincrona;
- fallimento della replica sincrona quando il secondario non e' raggiungibile.
"""

import socket
import subprocess
import sys
import time
from pathlib import Path


HOST = "127.0.0.1"
ASYNC_PRIMARY_PORT = 6390
SECONDARY_PORT = 6391
SYNC_PRIMARY_PORT = 6392
UNREACHABLE_REPLICATION_PORT = 6599

passed = 0
failed = 0


def wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} did not open within {timeout}s")


def request(command: str, port: int) -> str:
    with socket.create_connection((HOST, port), timeout=2.0) as connection:
        connection_file = connection.makefile("rwb")
        connection_file.write((command + "\n").encode("utf-8"))
        connection_file.flush()
        return connection_file.readline().decode("utf-8", errors="replace").strip()


def expect(command: str, port: int, prefix: str, test_name: str) -> str:
    global passed, failed
    response = request(command, port)
    ok = response.startswith(prefix)
    status = "PASS" if ok else "FAIL"
    print(f"  [{test_name}] {command} -> {response} [{status}]")
    if ok:
        passed += 1
    else:
        failed += 1
        print(f"    EXPECTED prefix: {prefix!r}")
    return response


def main() -> None:
    global passed, failed
    root = Path(__file__).resolve().parent
    processes: list[subprocess.Popen] = []

    try:
        print("=" * 60)
        print("SETUP: Starting primary-secondary nodes...")
        print("=" * 60)

        processes.append(
            subprocess.Popen(
                [
                    sys.executable,
                    str(root / "replica_secondary.py"),
                    "--port",
                    str(SECONDARY_PORT),
                    "--apply-delay",
                    "0.25",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        wait_for_port(SECONDARY_PORT)
        wait_for_port(SECONDARY_PORT + 100)
        print(f"  Secondary started on ports {SECONDARY_PORT}/{SECONDARY_PORT + 100}")

        processes.append(
            subprocess.Popen(
                [
                    sys.executable,
                    str(root / "primary_async.py"),
                    "--port",
                    str(ASYNC_PRIMARY_PORT),
                    "--secondary-port",
                    str(SECONDARY_PORT + 100),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        wait_for_port(ASYNC_PRIMARY_PORT)
        print(f"  Async primary started on port {ASYNC_PRIMARY_PORT}")

        processes.append(
            subprocess.Popen(
                [
                    sys.executable,
                    str(root / "primary_sync.py"),
                    "--port",
                    str(SYNC_PRIMARY_PORT),
                    "--secondary-port",
                    str(SECONDARY_PORT + 100),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        wait_for_port(SYNC_PRIMARY_PORT)
        print(f"  Sync primary started on port {SYNC_PRIMARY_PORT}")
        print()

        print("-" * 60)
        print("T1: Basic commands on async primary")
        print("-" * 60)
        expect("PING", ASYNC_PRIMARY_PORT, "OK PONG", "T1")
        expect("SET course ads", ASYNC_PRIMARY_PORT, "OK", "T1")
        expect("GET course", ASYNC_PRIMARY_PORT, "OK ads", "T1")
        expect("EXISTS course", ASYNC_PRIMARY_PORT, "OK 1", "T1")
        expect("INCR counter", ASYNC_PRIMARY_PORT, "OK 1", "T1")
        expect("DELETE course", ASYNC_PRIMARY_PORT, "OK", "T1")
        expect("GET course", ASYNC_PRIMARY_PORT, "NOT_FOUND", "T1")
        print()

        print("-" * 60)
        print("T2: Secondary is read-only and eventually receives async updates")
        print("-" * 60)
        expect("SET replicated value", ASYNC_PRIMARY_PORT, "OK", "T2")
        expect("SET should fail", SECONDARY_PORT, "ERR read-only secondary", "T2")
        time.sleep(0.6)
        expect("GET replicated", SECONDARY_PORT, "OK value", "T2")
        expect("KEYS", SECONDARY_PORT, "OK", "T2")
        print()

        print("-" * 60)
        print("T3: Sync primary waits for secondary ACK")
        print("-" * 60)
        expect("SET sync_key sync_value", SYNC_PRIMARY_PORT, "OK", "T3")
        time.sleep(0.3)
        expect("GET sync_key", SECONDARY_PORT, "OK sync_value", "T3")
        print()

        print("-" * 60)
        print("T4: Sync primary rejects writes without reachable secondary")
        print("-" * 60)
        unreachable_sync_port = 6393
        processes.append(
            subprocess.Popen(
                [
                    sys.executable,
                    str(root / "primary_sync.py"),
                    "--port",
                    str(unreachable_sync_port),
                    "--secondary-port",
                    str(UNREACHABLE_REPLICATION_PORT),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        wait_for_port(unreachable_sync_port)
        expect("SET blocked write", unreachable_sync_port, "ERR replica unavailable", "T4")
        print()

        total = passed + failed
        print("=" * 60)
        print(f"RESULTS: {passed}/{total} passed, {failed}/{total} failed")
        if failed == 0:
            print("ALL TESTS PASSED!")
        else:
            print("SOME TESTS FAILED!")
        print("=" * 60)
        raise SystemExit(1 if failed else 0)

    finally:
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
