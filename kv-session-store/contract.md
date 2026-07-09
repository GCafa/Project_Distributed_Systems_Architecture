# Public Contract

La strategia pubblica scelta e' stateless.

## GETV

Richiesta:

```text
GETV key MIN_VERSION m
```

Risposte:

```text
OK key value VERSION v
ERR not_found
ERR min_version_unavailable min_version=m best=b
ERR read_quorum_not_reached responses=n
ERR bad_request
```

Se la risposta e' `OK`, il coordinator garantisce sempre:

```text
v >= m
```

Se nessuna replica raggiungibile contiene una versione sufficiente, il sistema
non si blocca e risponde con `ERR min_version_unavailable`.

## SET

Richiesta:

```text
SET key value
```

Risposte:

```text
OK key value VERSION v
ERR write_quorum_not_reached acks=n
```

La versione `v` e' la nuova versione generata dal coordinator.

## CAS

Richiesta:

```text
CAS key expected_version value
```

Risposte:

```text
OK key value VERSION v
ERR cas_failed current=c
ERR not_found
ERR write_quorum_not_reached acks=n
ERR bad_request
```

`CAS` riesce solo se la versione corrente massima raggiungibile coincide con
`expected_version`.
