# Proprieta' di Safety e Liveness

## Safety

### S1: Read-Your-Writes per singolo client

> Se un client ha scritto `key=X` con `version=V`, una successiva
> `GETV key MIN_VERSION V` non puo' restituire una versione `< V`.

**Come e' garantita:**

Il client traccia internamente `last_seen_version[key]` e lo invia come
`MIN_VERSION` a ogni lettura. Il coordinator confronta la migliore versione
trovata nel quorum con `MIN_VERSION`:

```python
if min_version is not None and version < min_version:
    return f"ERR stale min_version={min_version} best={version}"
```

Se la condizione non e' soddisfatta, la lettura viene rifiutata con un errore
esplicito. Il client non riceve mai una versione piu' vecchia di quella che ha
gia' scritto.

---

### S2: Monotonic Reads per singolo client

> Se un client ha letto `key=X` con `version=V1`, una successiva lettura
> nella stessa sessione non puo' restituire `version=V2` con `V2 < V1`.

**Come e' garantita:**

Il client aggiorna la sessione con `max(current, received_version)` dopo ogni
lettura riuscita:

```python
session[key] = max(session.get(key, -1), version)
```

Nelle letture successive, `MIN_VERSION` sara' almeno `V1`, impedendo al
coordinator di restituire versioni inferiori.

---

### S3: Il read repair non sovrascrive versioni piu' nuove

> Il meccanismo di read repair invia aggiornamenti solo alle repliche con
> versione inferiore alla migliore trovata. La replica accetta la write
> solo se `incoming_version >= current_version`.

**Come e' garantita:**

Lato coordinator, il read repair invia write solo a repliche stale:

```python
if read_version < best_version:
    self._rpc(replica, {"type": "write", ...})
```

Lato replica, la write e' condizionata:

```python
if version >= current_version:
    self._data[key] = {"value": value, "version": version}
```

Questo doppio controllo garantisce che una versione piu' recente non venga
mai sovrascritta.

---

## Liveness

### L1: Progresso garantito se il quorum e' raggiungibile

> Un client corretto puo' sempre completare un'operazione se almeno R repliche
> (per letture) o W repliche (per scritture) sono raggiungibili e rispondono
> entro il timeout.

**Come e' garantita:**

Il coordinator itera sulle repliche con un timeout di 1 secondo per replica.
Se almeno R/W rispondono, l'operazione va a buon fine. Le repliche
irraggiungibili vengono semplicemente saltate.

---

### L2: Fallimento dichiarato (no blocco silenzioso)

> Se nessuna replica raggiungibile soddisfa `MIN_VERSION`, il coordinator
> risponde `ERR stale` anziche' bloccarsi indefinitamente.

**Come e' garantita:**

Il coordinator non implementa alcun meccanismo di attesa o polling. Dopo aver
raccolto le risposte dal quorum, confronta la migliore versione con
`MIN_VERSION`. Se non e' soddisfatta, restituisce immediatamente l'errore.

Il client puo' decidere come procedere:
- Riprovare dopo un breve ritardo (sperando che il read repair abbia propagato)
- Resettare la sessione (`RESET_SESSION`)
- Accettare l'errore e notificare l'utente

---

### L3: Nessuna memoria illimitata richiesta sul server

> La gestione della sessione non richiede stato sul server. Lo stato di
> sessione e' interamente lato client (una mappa `key -> version`).

**Come e' garantita:**

L'approccio stateless sposta completamente la responsabilita' sul client.
Il server non mantiene alcuna tabella di sessioni, nessun `client_id`,
nessuna storia di letture passate. Questo e' possibile perche' il client
invia esplicitamente `MIN_VERSION` a ogni richiesta.

**Trade-off:** Se il client perde il suo stato di sessione (crash, restart),
perde anche la garanzia di monotonic reads. Ma questo e' esattamente cio'
che ci si aspetta: la sessione e' legata al ciclo di vita del client.
