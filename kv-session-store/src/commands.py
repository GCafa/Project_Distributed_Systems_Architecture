from dataclasses import dataclass
import shlex

from .errors import BAD_REQUEST, EMPTY_COMMAND, UNKNOWN_COMMAND


@dataclass(frozen=True)
class Command:
    name: str
    args: tuple[str, ...]


def parse_command(line: str) -> Command | str:
    tokens = shlex.split(line)
    if not tokens:
        return EMPTY_COMMAND

    name = tokens[0].upper()
    args = tuple(tokens[1:])

    if name == "GET" and len(args) == 1:
        return Command(name, args)
    if name == "GETV" and len(args) in (1, 3):
        return Command(name, args)
    if name == "SET" and len(args) == 2:
        return Command(name, args)
    if name == "CAS" and len(args) == 3:
        return Command(name, args)

    if name in {"GET", "GETV", "SET", "CAS"}:
        return BAD_REQUEST
    return UNKNOWN_COMMAND
