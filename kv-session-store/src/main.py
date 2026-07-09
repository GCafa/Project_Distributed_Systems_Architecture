if __package__ in {None, ""}:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "src"

from .client import StatelessClient
from .coordinator import CoordinatorStateless
from .replica import ReplicaNode


def build_default_client() -> StatelessClient:
    replicas = [ReplicaNode("r1"), ReplicaNode("r2"), ReplicaNode("r3")]
    coordinator = CoordinatorStateless(replicas)
    return StatelessClient(coordinator)


def main() -> None:
    client = build_default_client()
    print("Stateless KV client. Commands: GET key, GETV key, SET key value, CAS key version value")
    while True:
        try:
            line = input("> ")
        except EOFError:
            break

        if line.strip().upper() in {"QUIT", "EXIT"}:
            break

        print(client.send(line))


if __name__ == "__main__":
    main()
