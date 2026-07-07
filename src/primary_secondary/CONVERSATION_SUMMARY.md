# Conversation Summary

## Goal

Analyze the existing lab implementation of a primary-secondary replicated KV
store and implement the same system in:

```text
C:\Users\Ph3\Documents\GitHub\Project_Distributed_Systems_Architecture\src\primary_secondary
```

## Source Folder Analyzed

Original folder:

```text
C:\Users\Ph3\Documents\GitHub\architetture-dei-sistemi-distribuiti-2025-2026\labs\kv_store\replication_primary_secondary
```

Files reviewed:

- `client.py`
- `primary_async.py`
- `primary_sync.py`
- `replica_secondary.py`
- `README.md`

## Architecture Found

The source lab implements a Python TCP key-value store with one primary and one
secondary replica.

Main components:

- `primary_async.py`: applies writes locally, replies `OK` immediately, then
  replicates to the secondary in a background thread.
- `primary_sync.py`: sends writes to the secondary first and replies `OK` only
  after receiving `ACK`.
- `replica_secondary.py`: exposes a read-only client endpoint and a separate
  internal replication endpoint.
- `client.py`: interactive line-based TCP client.

Default ports:

- async primary: `6390`
- secondary client endpoint: `6391`
- secondary replication endpoint: `6491`
- sync primary: `6392`

Client commands on primary:

```text
PING
SET <key> <value...>
GET <key>
DELETE <key>
EXISTS <key>
KEYS
INCR <key>
QUIT
```

Client commands on secondary:

```text
PING
GET <key>
EXISTS <key>
KEYS
QUIT
```

The secondary is deliberately read-only for clients.

## Target Project Context

Target project inspected:

```text
C:\Users\Ph3\Documents\GitHub\Project_Distributed_Systems_Architecture
```

Existing relevant structure:

- `src/quorum/acceptance_test.py`
- `src/quorum/client.py`
- `src/quorum/coordinator.py`
- `src/quorum/replica_node.py`
- root docs: `README.md`, `CONTRACT.md`, `SAFETY_LIVENESS.md`,
  `TECHNICAL_NOTE.md`

The target project already used Python TCP servers, line-based client commands,
JSON internal messages, and acceptance tests that start subprocesses.

## Implementation Added

Created these files in `src/primary_secondary`:

- `client.py`
- `primary_async.py`
- `primary_sync.py`
- `replica_secondary.py`
- `acceptance_test.py`
- `README.md`

The implementation preserves the original lab behavior while matching the
target repository style with clearer docstrings, comments, and an automated
acceptance test.

## Test-First Work

An acceptance test was added before production code. It initially failed because
the primary-secondary server files did not exist yet:

```text
RuntimeError: port 6391 did not open within 5.0s
```

After implementation, the acceptance test passed.

## Verification

Final verification command:

```powershell
uv run python src/primary_secondary/acceptance_test.py
```

Final result:

```text
RESULTS: 14/14 passed, 0/14 failed
ALL TESTS PASSED!
```

Additional compile check also passed with `py_compile` over all Python files in
`src/primary_secondary`.

## Git Status Note

After the work, git showed:

```text
?? .idea/
?? src/primary_secondary/
```

Only `src/primary_secondary/` was created for this task. The `.idea/` directory
was already untracked and was not modified.
