# KV Store con Replica Primary-Secondary

Questa cartella implementa un KV store con un primary e un secondario.

Il focus del laboratorio e' la differenza tra replica asincrona e replica
sincrona:

- `primary_async.py`: risponde `OK` dopo l'update locale e replica dopo;
- `primary_sync.py`: risponde `OK` solo dopo `ACK` del secondario;
- `replica_secondary.py`: serve letture ai client e riceve update dal primary;
- `client.py`: client interattivo;
- `acceptance_test.py`: test automatico end-to-end.

## Porte di default

| Nodo | Porta |
| --- | --- |
| Primary async | `6390` |
| Secondary client read-only | `6391` |
| Secondary replication endpoint | `6491` |
| Primary sync | `6392` |

Il secondario espone il canale di replica su `--port + 100`.

## Comandi client

Sui primary:

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

Sul secondario:

```text
PING
GET <key>
EXISTS <key>
KEYS
QUIT
```

Il secondario e' deliberatamente read-only per i client.

## Avvio manuale

Da questa cartella:

```bash
python replica_secondary.py --apply-delay 1.0
python primary_async.py
python primary_sync.py
python client.py --port 6390
```

## Test

```bash
python acceptance_test.py
```

Il test avvia automaticamente i processi necessari, verifica i casi principali
e li spegne alla fine.
