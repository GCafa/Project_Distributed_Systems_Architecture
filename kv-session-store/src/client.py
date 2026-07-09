from .commands import Command, parse_command
from .coordinator import CoordinatorStateless
from .errors import BAD_REQUEST


class StatelessClient:
    def __init__(self, coordinator: CoordinatorStateless) -> None:
        self.coordinator = coordinator
        self.min_versions: dict[str, int] = {}

    def build_getv_command(self, key: str) -> str:
        return f"GETV {key} MIN_VERSION {self.min_versions.get(key, 0)}"

    def get(self, key: str) -> str:
        return self.coordinator.get(key)

    def getv(self, key: str) -> str:
        min_version = self.min_versions.get(key, 0)
        response = self.coordinator.getv(key, min_version)
        self._remember_version_from_response(key, response)
        return response

    def set(self, key: str, value: str) -> str:
        response = self.coordinator.set(key, value)
        self._remember_version_from_response(key, response)
        return response

    def cas(self, key: str, expected_version: int, value: str) -> str:
        response = self.coordinator.cas(key, expected_version, value)
        self._remember_version_from_response(key, response)
        return response

    def send(self, line: str) -> str:
        command = parse_command(line)
        if isinstance(command, str):
            return command

        return self._execute(command)

    def _execute(self, command: Command) -> str:
        if command.name == "GET":
            return self.get(command.args[0])

        if command.name == "GETV":
            key = command.args[0]
            if len(command.args) == 3:
                if command.args[1].upper() != "MIN_VERSION":
                    return BAD_REQUEST
                try:
                    requested_min = int(command.args[2])
                except ValueError:
                    return BAD_REQUEST
                self.min_versions[key] = max(
                    self.min_versions.get(key, 0),
                    requested_min,
                )
            return self.getv(key)

        if command.name == "SET":
            key, value = command.args
            return self.set(key, value)

        if command.name == "CAS":
            key, expected_version, value = command.args
            try:
                expected = int(expected_version)
            except ValueError:
                return BAD_REQUEST
            return self.cas(key, expected, value)

        return BAD_REQUEST

    def _remember_version_from_response(self, key: str, response: str) -> None:
        parts = response.split()
        if len(parts) < 5 or parts[0] != "OK" or parts[-2] != "VERSION":
            return

        try:
            version = int(parts[-1])
        except ValueError:
            return

        self.min_versions[key] = max(self.min_versions.get(key, 0), version)
