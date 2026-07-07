# Proprieta' di Safety e Liveness - Primary-Secondary

## Safety

### S1: Il secondario non accetta scritture dai client

**Proprieta':**

Un client connesso all'endpoint pubblico del secondario non puo' modificare lo
stato del KV store.

**Come e' garantita:**

Il secondario registra handler client solo per:

```text
PING
GET
EXISTS
KEYS
QUIT
```

Qualunque altro comando sul client endpoint del secondario restituisce:

```text
ERR read-only secondary
```

Le mutazioni sono accettate solo dal canale interno di replica, che usa record
JSON con operazioni `SET`, `DELETE` e `INCR`.

**Limite:**

Il canale di replica non autentica il primary. In un sistema reale servirebbe
autenticazione o isolamento di rete.

### S2: Il primary sync non conferma scritture non replicate

**Proprieta':**

Nel primary sincrono, una risposta `OK` a `SET`, `DELETE` o `INCR` implica che
il secondario ha risposto `ACK` per quell'operazione.

**Come e' garantita:**

Il primary sync chiama `_replicate_and_ack(...)` prima di aggiornare lo stato
locale. Se il secondario e' irraggiungibile o risponde con un errore, il primary
restituisce errore al client e non applica la mutazione locale.

**Conseguenza:**

La semantica di commit e' piu' forte: una scrittura confermata e' presente sia
sul primary sia sul secondario, salvo crash successivi non persistiti.

### S3: Le operazioni locali sono protette da lock

**Proprieta':**

Accessi concorrenti allo stato locale di ciascun nodo non corrompono il
dizionario in memoria.

**Come e' garantita:**

`AsyncPrimaryStore`, `SyncPrimaryStore` e `SecondaryStore` proteggono `_data`
con un `threading.Lock` durante letture e scritture locali.

**Limite:**

Il lock protegge il singolo processo, non fornisce un ordine globale tra nodi.
Nel primary async, la replica avviene dopo l'update locale e puo' arrivare in
ritardo rispetto alle letture sul secondario.

## Liveness

### L1: Il primary async puo' progredire anche se il secondario non risponde

**Proprieta':**

Il primary asincrono puo' completare una scrittura locale anche quando il
secondario e' non raggiungibile.

**Come e' garantita:**

La replica e' eseguita in un thread daemon separato. Il client riceve `OK` dopo
l'update locale; eventuali errori di replica vengono solo loggati.

**Costo:**

La disponibilita' aumenta, ma una scrittura confermata al client puo' non essere
mai arrivata al secondario.

### L2: Il primary sync fallisce esplicitamente invece di bloccare indefinitamente

**Proprieta':**

Se il secondario non e' raggiungibile, il primary sincrono non resta bloccato
indefinitamente.

**Come e' garantita:**

La connessione verso il secondario usa un timeout di 1 secondo. In caso di
errore o timeout, il client riceve:

```text
ERR replica unavailable: <reason>
```

**Costo:**

La disponibilita' delle scritture dipende dal secondario. Se il secondario e'
down, il primary sync non accetta mutazioni.

### L3: Il secondario puo' convergere quando riceve gli update

**Proprieta':**

Se il primary riesce a inviare gli update e il secondario li processa, lo stato
del secondario converge verso quello del primary per le chiavi replicate.

**Come e' garantita:**

Ogni mutazione del primary genera un record di replica:

- `SET` copia il valore;
- `DELETE` rimuove la chiave;
- `INCR` invia il valore numerico gia' calcolato dal primary.

Il parametro `--apply-delay` puo' ritardare l'applicazione degli update per
rendere osservabile la finestra di staleness.

**Limite:**

Non esiste una coda persistente di replica. Se un update async fallisce, non
viene ritentato automaticamente.

## Trade-off Safety/Liveness

| Scelta | Safety | Liveness |
| --- | --- | --- |
| Replica asincrona | Puo' confermare update non replicati | Continua anche senza secondario |
| Replica sincrona | `OK` implica ACK del secondario | Scritture non disponibili senza secondario |
| Secondario read-only | Evita divergenza da scritture client | Non permette failover automatico |
| Stato in memoria | Semplice da verificare nel lab | Perde dati su crash |
