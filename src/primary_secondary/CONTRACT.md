# Contratto Pubblico - KV Store Primary-Secondary

## Panoramica

Il sistema implementa un KV store replicato con topologia primary-secondary.

Esistono due varianti di primary:

- `primary_async.py`: applica la scrittura localmente e risponde `OK` prima
  dell'ACK del secondario.
- `primary_sync.py`: risponde `OK` solo dopo avere ricevuto `ACK` dal
  secondario.

Il secondario (`replica_secondary.py`) e' read-only per i client. Le mutazioni
arrivano solo dal canale interno di replica.

## Endpoint

| Nodo | Porta default | Protocollo |
| --- | --- | --- |
| Primary async | `6390` | Comandi testuali client |
| Secondary client endpoint | `6391` | Comandi testuali read-only |
| Secondary replication endpoint | `6491` | JSON interno, una riga per record |
| Primary sync | `6392` | Comandi testuali client |

Il secondario espone il canale di replica su `--port + 100`.

## Protocollo Client

Ogni comando e' una riga UTF-8 terminata da newline. Ogni risposta e' una riga
UTF-8 terminata da newline.

### PING

```text
PING
```

**Risposta:**

- `OK PONG`
- `ERR usage: PING` se sono presenti argomenti

### SET

```text
SET <key> <value>
```

Scrive `value` nella chiave `key`. Il valore e' tutto cio' che resta dopo il
primo spazio successivo alla chiave.

**Disponibilita':** solo primary.

**Precondizioni:**

- `key` non deve essere vuota
- `key` non deve contenere spazi
- `value` deve essere presente

**Postcondizioni primary async:**

- Il primary aggiorna subito lo stato locale.
- Il primary prova a replicare l'update in background.
- La risposta al client non dipende dall'esito della replica.

**Postcondizioni primary sync:**

- Il primary invia l'update al secondario.
- Se il secondario risponde `ACK`, il primary aggiorna lo stato locale.
- Se il secondario non risponde o rifiuta, lo stato locale non viene aggiornato.

**Risposte:**

- `OK`
- `ERR usage: SET <key> <value>`
- `ERR replica unavailable: <reason>` sul primary sync
- `ERR replica rejected update: <reason>` sul primary sync
- `ERR read-only secondary` se inviato al secondario

### GET

```text
GET <key>
```

Legge una chiave dal nodo a cui il client e' connesso.

**Disponibilita':** primary e secondario.

**Risposte:**

- `OK <value>`
- `NOT_FOUND`
- `ERR usage: GET <key>`

### DELETE

```text
DELETE <key>
```

Rimuove una chiave.

**Disponibilita':** solo primary.

**Postcondizioni primary async:**

- Il primary elimina subito la chiave localmente.
- La replica del delete e' tentata in background.

**Postcondizioni primary sync:**

- Il primary elimina localmente solo dopo `ACK` del secondario.

**Risposte:**

- `OK`
- `NOT_FOUND`
- `ERR usage: DELETE <key>`
- `ERR replica unavailable: <reason>` sul primary sync
- `ERR replica rejected update: <reason>` sul primary sync
- `ERR read-only secondary` se inviato al secondario

### EXISTS

```text
EXISTS <key>
```

Verifica se una chiave e' presente nel nodo interrogato.

**Disponibilita':** primary e secondario.

**Risposte:**

- `OK 1` se la chiave esiste
- `OK 0` se la chiave non esiste
- `ERR usage: EXISTS <key>`

### KEYS

```text
KEYS
```

Restituisce le chiavi locali ordinate lessicograficamente.

**Disponibilita':** primary e secondario.

**Risposte:**

- `OK <key1> <key2> ...`
- `OK` se non ci sono chiavi
- `ERR usage: KEYS` se sono presenti argomenti

### INCR

```text
INCR <key>
```

Incrementa un valore intero. Se la chiave non esiste, il valore iniziale e'
considerato `0`.

**Disponibilita':** solo primary.

**Postcondizioni primary async:**

- Il primary aggiorna subito il valore locale.
- La replica dell'incremento e' tentata in background.

**Postcondizioni primary sync:**

- Il primary aggiorna localmente solo dopo `ACK` del secondario.

**Risposte:**

- `OK <new_value>`
- `ERR usage: INCR <key>`
- `ERR value is not an integer`
- `ERR replica unavailable: <reason>` sul primary sync
- `ERR replica rejected update: <reason>` sul primary sync
- `ERR read-only secondary` se inviato al secondario

### QUIT

```text
QUIT
```

Chiude la connessione dopo la risposta.

**Risposte:**

- `OK BYE`
- `ERR usage: QUIT` se sono presenti argomenti

## Protocollo Interno di Replica

Il primary apre una connessione TCP verso il replication endpoint del
secondario, invia un record JSON seguito da newline e attende una riga di
risposta.

Record ammessi:

```json
{"op": "SET", "key": "course", "value": "ads"}
{"op": "DELETE", "key": "course"}
{"op": "INCR", "key": "counter", "value": "1"}
```

Risposte del secondario:

- `ACK`
- `ERR invalid json`
- `ERR invalid replication record`
- `ERR replica value is not an integer`

## Casi Fuori Contratto

| Situazione | Comportamento |
| --- | --- |
| Comando sconosciuto | `ERR unknown command` |
| Comando vuoto | `ERR empty command` |
| Scrittura inviata al secondario client endpoint | `ERR read-only secondary` |
| Secondario non raggiungibile con primary async | Il primary risponde comunque `OK` e logga il fallimento |
| Secondario non raggiungibile con primary sync | La scrittura fallisce con `ERR replica unavailable: ...` |
| Lettura dal secondario subito dopo `OK` async | Puo' restituire stato stantio |
| Crash del processo | I dati in memoria vengono persi |
| Due primary attivi contemporaneamente | Fuori contratto; non esiste coordinamento anti split-brain |
