# KV Session Store Stateless

Questo progetto implementa un key-value store distribuito con session consistency
lato client usando la strategia stateless `MIN_VERSION`.

## Struttura

```text
src/
  main.py              Avvia un client interattivo di esempio.
  client.py            Client stateless: mantiene min_versions[key].
  coordinator.py       Server esposto ai client e coordinatore delle repliche.
  replica.py           Nodo replica con storage locale key -> VersionedValue.
  versioned_value.py   Dataclass value/version.
  commands.py          Parsing dei comandi testuali.
  errors.py            Errori pubblici centralizzati.

tests/
  test_min_version_contract.py
  test_client_stateless_session.py
  test_read_your_writes.py
```

Il server pubblico e' `coordinator.py`. Le repliche sono server interni usati dal
coordinator.

## Comandi

```text
GET key
GETV key
GETV key MIN_VERSION m
SET key value
CAS key expected_version value
```

Il client usa sempre la propria tabella locale `min_versions` quando esegue
`GETV`. Dopo ogni `GETV`, `SET` o `CAS` riuscito aggiorna:

```text
min_versions[key] = max(min_versions[key], version_received)
```

## Test

Da questa cartella:

```powershell
python -m unittest discover -s tests -v
```

I test principali verificano che:

- `GETV key MIN_VERSION m` non restituisca mai una versione `< m`;
- se nessuna replica raggiungibile soddisfa `m`, ritorni un errore dichiarato;
- il client non osservi versioni piu' vecchie dopo una lettura o scrittura;
- `SET` e `CAS` restituiscano la nuova versione.
