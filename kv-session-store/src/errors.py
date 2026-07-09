EMPTY_COMMAND = "ERR empty command"
UNKNOWN_COMMAND = "ERR unknown command"
BAD_REQUEST = "ERR bad_request"

NOT_FOUND = "ERR not_found"
MIN_VERSION_UNAVAILABLE = "ERR min_version_unavailable"

READ_QUORUM_NOT_REACHED = "ERR read_quorum_not_reached"
WRITE_QUORUM_NOT_REACHED = "ERR write_quorum_not_reached"

CAS_FAILED = "ERR cas_failed"
VERSION_MISMATCH = "ERR version_mismatch"

INVALID_JSON = "ERR invalid_json"
MISSING_KEY = "ERR missing_key"
STALE_WRITE = "ERR stale_write"


def err_min_version_unavailable(min_version: int, best: int) -> str:
    return f"{MIN_VERSION_UNAVAILABLE} min_version={min_version} best={best}"


def err_read_quorum_not_reached(responses: int) -> str:
    return f"{READ_QUORUM_NOT_REACHED} responses={responses}"


def err_write_quorum_not_reached(acks: int) -> str:
    return f"{WRITE_QUORUM_NOT_REACHED} acks={acks}"


def err_version_mismatch(current: int) -> str:
    return f"{VERSION_MISMATCH} current={current}"


def err_cas_failed(current: int) -> str:
    return f"{CAS_FAILED} current={current}"
