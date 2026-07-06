# Nota Tecnica — Trade-off e Limiti

## La Promessa

Il nostro sistema introduce una garanzia di **session consistency** per singolo
client: dopo aver letto o scritto una versione V di una chiave, il client non
osservera' mai una versione precedente a V nella stessa sessione.

Questa garanzia e' piu' debole della linearizzabilita' (non ordiniamo le
operazioni tra client diversi) ma significativamente piu' utile della
eventuale consistenza pura, perche' elimina le anomalie piu' confuse per
l'utente finale: leggere un dato, aggiornarlo, e poi vederlo "tornare
indietro".

## Costo Tecnico

### 1. Stateless vs Stateful

| Aspetto | Stateless (MIN_VERSION) | Stateful (SESSION client_id) |
|---|---|---|
| Stato sul server | Nessuno | Tabella `client_id -> {key -> version}` |
| Scalabilita' | Ottima (nessun stato da replicare) | Limitata (stato cresce con i client) |
| Crash del client | Perde la sessione (accettabile) | Il server tiene stato orfano |
| Complessita' client | Deve tracciare le versioni | Semplice (invia solo client_id) |

**Scelta:** Stateless. Il costo e' un client leggermente piu' complesso,
ma il server resta completamente stateless e scalabile.

### 2. Errore Esplicito vs Attesa

Quando una lettura con `MIN_VERSION` non e' soddisfacibile, il sistema
potrebbe:

- (a) Attendere che una replica si aggiorni (polling/blocking)
- (b) Restituire un errore esplicito (`ERR stale`)

**Scelta:** (b) Errore esplicito. Motivazioni:
- Nessun rischio di blocco indefinito
- Il client ha il controllo sulla politica di retry
- Semantica piu' chiara e debuggabile
- Nessun timeout nascosto

### 3. Read Repair in Background

Il read repair avviene in un thread separato dopo aver risposto al client.
Questo non rallenta la latenza della lettura, ma introduce un ritardo nella
convergenza delle repliche.

**Trade-off:** Se un client legge immediatamente dopo, le repliche stale
potrebbero non essere ancora aggiornate. Ma il `MIN_VERSION` protegge
comunque la sessione: il client ricevera' `ERR stale` e potra' riprovare.

### 4. Versioni Monotone dal Coordinator

Le versioni sono assegnate dal coordinator come `max_corrente + 1`.
Questo crea un ordinamento totale semplice e comprensibile.

**Limite:** Il coordinator e' un single point of assignment. Se ci fossero
piu' coordinator concorrenti, servirebbero timestamp logici o vettoriali.
Ma nel nostro modello con un singolo coordinator, questo non e' un problema.

## Limiti Rimasti

1. **Single coordinator:** Se il coordinator crolla, il sistema e'
   indisponibile. Un'estensione potrebbe introdurre un lease-based failover
   (Homework 4).

2. **Nessuna persistenza:** Le repliche mantengono i dati in memoria.
   Un crash di una replica perde i suoi dati. Il read repair compensa
   parzialmente, ma un crash simultaneo di tutte le repliche perde tutto.

3. **Connessione TCP per operazione:** Il coordinator apre una nuova
   connessione TCP verso ogni replica per ogni operazione. Questo e'
   semplice ma non efficiente. Un connection pool sarebbe piu' performante.

4. **MIN_VERSION per singola chiave:** La garanzia e' per singola chiave.
   Non garantiamo consistenza causale tra chiavi diverse (es. "se ho scritto
   A=1 e B=2, un altro client potrebbe vedere B=2 e A=0").

## Possibili Evoluzioni

1. **Session consistency cross-key:** Usare un vettore di versioni globale
   anziche' per-key, per garantire causalita' tra chiavi diverse.

2. **Retry automatico:** Il client potrebbe riprovare automaticamente dopo
   `ERR stale`, con backoff esponenziale, prima di restituire l'errore
   all'utente.

3. **Persistent storage:** Salvare i dati su disco con un WAL (Write-Ahead
   Log) per sopravvivere ai crash delle repliche.

4. **Connection pooling:** Riutilizzare le connessioni TCP verso le repliche.

5. **Anti-entropy background:** Un processo periodico che confronta le repliche
   e propaga le versioni mancanti, complementare al read repair.
