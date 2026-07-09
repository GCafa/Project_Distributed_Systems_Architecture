from dataclasses import dataclass


@dataclass(frozen=True)
class VersionedValue:
    value: str
    version: int

    def __post_init__(self) -> None:
        if self.version < 0:
            raise ValueError("version must be non-negative")
