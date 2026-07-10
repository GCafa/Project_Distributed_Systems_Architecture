#!/usr/bin/env python3
"""
Launcher eseguibile per il progetto quorum stateful.

Avvia 3 repliche, il coordinator stateful e poi apre il client
interattivo sulla stessa console. Quando il client termina, spegne il cluster.
"""

import argparse
import socket
import subprocess
import sys
import time
from pathlib import Path


HOST = "127.0.0.1"
COORDINATOR_PORT = 6430
REPLICA_PORTS = [6431, 6432, 6433]
SESSION_GC_TIMEOUT = 300.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Avvia il cluster quorum stateful e il client interattivo"
    )
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--coordinator-port", type=int, default=COORDINATOR_PORT)
    parser.add_argument(
        "--replica-ports",
        type=int,
        nargs=3,
        default=REPLICA_PORTS,
        metavar=("R0_PORT", "R1_PORT", "R2_PORT"),
    )
    parser.add_argument("--read-quorum", type=int, default=2)
    parser.add_argument("--write-quorum", type=int, default=2)
    parser.add_argument("--client-id", default="clientA")
    parser.add_argument("--session-gc-timeout", type=float, default=SESSION_GC_TIMEOUT)
    parser.add_argument(
        "--show-server-logs",
        action="store_true",
        help="Mostra i log di repliche e coordinator nella console",
    )
    return parser.parse_args()


def wait_for_port(host: str, port: int, process: subprocess.Popen, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"processo terminato prima di aprire {host}:{port}")
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"{host}:{port} non si e' aperta entro {timeout}s")


def ensure_port_available(host: str, port: int) -> None:
    try:
        with socket.create_connection((host, port), timeout=0.2):
            raise RuntimeError(f"{host}:{port} e' gia' in uso")
    except OSError:
        return


def start_server(
    command: list[str],
    root: Path,
    show_logs: bool,
) -> subprocess.Popen:
    output = None if show_logs else subprocess.DEVNULL
    return subprocess.Popen(
        command,
        cwd=root,
        stdin=subprocess.DEVNULL,
        stdout=output,
        stderr=output,
    )


def stop_processes(processes: list[subprocess.Popen]) -> None:
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
    for process in reversed(processes):
        try:
            process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2.0)


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent
    processes: list[subprocess.Popen] = []

    ports = [args.coordinator_port, *args.replica_ports]
    for port in ports:
        ensure_port_available(args.host, port)

    try:
        for index, port in enumerate(args.replica_ports):
            node_id = f"R{index}"
            process = start_server(
                [
                    sys.executable,
                    str(root / "replica_node.py"),
                    "--node-id",
                    node_id,
                    "--host",
                    args.host,
                    "--port",
                    str(port),
                ],
                root,
                args.show_server_logs,
            )
            processes.append(process)
            wait_for_port(args.host, port, process)
            print(f"Replica {node_id} pronta su {args.host}:{port}", flush=True)

        replica_args = [f"{args.host}:{port}" for port in args.replica_ports]
        coordinator = start_server(
            [
                sys.executable,
                str(root / "coordinator.py"),
                "--host",
                args.host,
                "--port",
                str(args.coordinator_port),
                "--read-quorum",
                str(args.read_quorum),
                "--write-quorum",
                str(args.write_quorum),
                "--session-gc-timeout",
                str(args.session_gc_timeout),
                "--replicas",
                *replica_args,
            ],
            root,
            args.show_server_logs,
        )
        processes.append(coordinator)
        wait_for_port(args.host, args.coordinator_port, coordinator)
        print(
            f"Coordinator stateful pronto su {args.host}:{args.coordinator_port}",
            flush=True,
        )
        print("Client interattivo avviato. Usa QUIT per uscire.\n", flush=True)

        client = subprocess.Popen(
            [
                sys.executable,
                str(root / "client.py"),
                "--host",
                args.host,
                "--port",
                str(args.coordinator_port),
                "--client-id",
                args.client_id,
            ],
            cwd=root,
        )
        return client.wait()
    except KeyboardInterrupt:
        print("\nInterruzione ricevuta, arresto del cluster...")
        return 130
    finally:
        stop_processes(processes)


if __name__ == "__main__":
    sys.exit(main())
