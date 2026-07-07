# Contratto Pubblico — Session Consistency Per Client

## Panoramica

Questo sistema estende un KV store con quorum aggiungendo una garanzia di
**consistenza di sessione**: un client che ha scritto o letto una certa versione
di una chiave non potra' mai osservare, nella stessa sessione, una versione piu'
vecchia di quella chiave.

L'approccio e' **stateless**: il client invia `MIN_VERSION` a ogni richiesta di
lettura. Il server non mantiene alcuna informazione sulle sessioni.

---

## Comandi

### SET

```
SET <key> <value>
```

Scrive un valore associato alla chiave. Il coordinator assegna automaticamente
una versione monotona crescente (`max_corrente + 1`).

**Precondizioni:**
- `key` non deve contenere spazi
- `value` e' la parte rimanente della riga dopo il primo spazio

**Postcondizioni:**
- Il valore e' scritto su almeno W repliche
- La versione assegnata e' strettamente maggiore di qualunque versione precedente

**Risposta:**
- `OK version=<v> acks=<n>` — scrittura riuscita
- `ERR write quorum not reached acks=<n>` — meno di W repliche hanno confermato

---

### GETV

```
GETV <key> [MIN_VERSION <v>]
```

Legge il valore e la versione di una chiave.

Se `MIN_VERSION` e' specificato, il coordinator verifica che la migliore versione
trovata nel quorum sia `>= v`. In caso contrario, restituisce un errore esplicito.

**Precondizioni:**
- `key` non deve contenere spazi
- `MIN_VERSION`, se presente, deve essere un intero >= 0

**Postcondizioni:**
- Se la chiave esiste e la versione soddisfa `MIN_VERSION`: restituisce valore e versione
- Se la chiave esiste ma la versione e' inferiore a `MIN_VERSION`: errore `stale`
- Se la chiave non esiste: `NOT_FOUND`

**Risposta:**
- `OK <value> version=<v>` — lettura riuscita
- `NOT_FOUND` — chiave inesistente
- `ERR stale min_version=<v> best=<b>` — la versione migliore disponibile e' inferiore al minimo richiesto
- `ERR read quorum not reached responses=<n>` — meno di R repliche hanno risposto

**Effetto collaterale:**
Dopo una lettura riuscita, il coordinator esegue **read repair** in background:
le repliche con versioni inferiori alla migliore vengono aggiornate.

---

### CAS (Compare-And-Swap)

```
CAS <key> <expected_version> <new_value>
```

Operazione atomica: scrive `new_value` solo se la versione corrente della chiave
e' esattamente `expected_version`. La nuova versione sara' `expected_version + 1`.

**Precondizioni:**
- La chiave deve gia' esistere
- `expected_version` deve essere un intero >= 0

**Postcondizioni:**
- Se `current_version == expected_version`: il valore viene aggiornato
- Altrimenti: nessuna modifica

**Risposta:**
- `OK version=<v> acks=<n>` — CAS riuscita
- `ERR version_mismatch current=<v>` — la versione corrente non corrisponde
- `ERR not_found` — la chiave non esiste
- `ERR write quorum not reached acks=<n>` — quorum non raggiunto

---

### GET

```
GET <key>
```

Lettura leggera: interroga solo R repliche (senza read repair) e restituisce
valore e versione.

**Non supporta `MIN_VERSION`.** Per letture con garanzia di session consistency,
usare `GETV`.

**Risposta:**
- `OK <value> version=<v>` — lettura riuscita
- `NOT_FOUND` — chiave inesistente
- `ERR read quorum not reached responses=<n>` — quorum non raggiunto

---

### PING

```
PING
```

**Risposta:** `OK PONG`

---

### STATUS

```
STATUS
```

**Risposta:** `OK N=<n> R=<r> W=<w>`

---

### QUIT

```
QUIT
```

**Risposta:** `OK BYE` (chiude la connessione)

---

## Casi Fuori Contratto

| Situazione | Comportamento |
|---|---|
| `MIN_VERSION` > qualunque versione mai scritta | `ERR stale` — il client puo' decidere se riprovare o resettare la sessione |
| Tutte le repliche irraggiungibili | `ERR read/write quorum not reached` |
| Chiave con spazi nel nome | Comportamento non definito |
| Richiesta malformata | `ERR usage: ...` con messaggio di aiuto |
| Versione non intera in MIN_VERSION | `ERR MIN_VERSION must be an integer` |
