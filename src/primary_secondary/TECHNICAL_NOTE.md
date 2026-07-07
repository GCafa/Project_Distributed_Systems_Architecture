# Nota Tecnica - KV Store Primary-Secondary

## Promessa del Sistema

Il sistema introduce una replica secondaria per mostrare come cambia il
significato di una scrittura quando lo stato deve essere propagato su piu'
nodi.

La promessa dipende dal tipo di primary scelto:

- con `primary_async.py`, `OK` significa "scrittura applicata localmente sul
  primary";
- con `primary_sync.py`, `OK` significa "scrittura applicata sul primary solo
  dopo ACK del secondario".

Questa distinzione e' il punto centrale del laboratorio: "visibile sul primary"
e "replicato" non sono la stessa cosa.

## Scelte Implementative

### 1. Protocollo client testuale

Il protocollo client e' line-based e leggibile:

```text
SET course ads
GET course
INCR counter
```

**Vantaggi:**

- facile da provare manualmente;
- coerente con i laboratori KV store;
- non richiede dipendenze esterne.

**Costo:**

- parsing minimale;
- nessun escaping strutturato per chiavi con spazi;
- risposte non tipizzate.

### 2. Protocollo di replica JSON

Il canale interno usa JSON newline-delimited:

```json
{"op": "SET", "key": "course", "value": "ads"}
```

**Vantaggi:**

- piu' esplicito del protocollo testuale;
- semplice da estendere con campi futuri;
- separa bene traffico client e traffico interno.

**Costo:**

- non c'e' autenticazione del mittente;
- non c'e' framing oltre al newline;
- non c'e' persistenza dei record inviati.

### 3. Due primary separati

`primary_async.py` e `primary_sync.py` sono due programmi distinti invece di un
solo primary parametrico.

**Vantaggi:**

- rende chiaro il confronto didattico;
- ogni file mostra una sola semantica di commit;
- i test possono avviare entrambi i comportamenti in parallelo.

**Costo:**

- duplicazione tra i due file;
- eventuali nuovi comandi vanno aggiornati in entrambe le varianti.

### 4. Stato in memoria

Ogni nodo mantiene un dizionario Python protetto da lock.

**Vantaggi:**

- implementazione breve e leggibile;
- adatta a test di protocollo e semantica;
- nessuna dipendenza da database o filesystem.

**Costo:**

- i dati si perdono al crash del processo;
- non c'e' recupero dopo riavvio;
- non esiste log di replica o replay.

## Limiti Rimasti

### Nessun failover

Il secondario non puo' diventare primary. Se il primary cade, il sistema non
elegge un nuovo leader.

Una possibile evoluzione e' introdurre lease, heartbeat e promozione controllata
del secondario.

### Nessun anti split-brain

Il sistema assume che esista un solo primary attivo. Se due processi primary
scrivono verso lo stesso secondario, non esiste un protocollo per ordinare o
risolvere conflitti.

Una possibile evoluzione e' aggiungere epoche di leadership o fencing token.

### Nessun retry persistente per replica async

Nel primary async, un errore di replica viene loggato ma non ritentato. Quindi
una scrittura confermata al client puo' rimanere solo sul primary.

Una possibile evoluzione e' usare una coda locale di replica con retry e ack
tracking.

### Nessuna versione per chiave

Il protocollo non assegna versioni alle scritture. Il secondario applica gli
update nell'ordine in cui li riceve.

Una possibile evoluzione e' aggiungere numeri di sequenza monotoni per rifiutare
update vecchi o duplicati.

### Nessuna persistenza

Primary e secondario perdono lo stato a ogni crash. Una possibile evoluzione e'
aggiungere un WAL locale prima di confermare scritture.

## Test Ripetibili

Il file `acceptance_test.py` avvia automaticamente:

- un secondario;
- un primary asincrono;
- un primary sincrono;
- un primary sincrono aggiuntivo puntato a una porta di replica irraggiungibile.

Copre:

- comandi base sul primary async;
- rifiuto delle scritture sul secondario;
- convergenza della replica asincrona;
- ACK richiesto dal primary sync;
- errore esplicito del primary sync senza secondario raggiungibile.

Comando:

```bash
python acceptance_test.py
```

## Possibili Evoluzioni

1. Aggiungere un WAL per durabilita' locale.
2. Aggiungere una coda di replica persistente per il primary async.
3. Aggiungere versioni o sequence number per ogni update.
4. Implementare heartbeat e failover del secondario.
5. Separare logica comune dei primary in un modulo condiviso.
6. Aggiungere autenticazione sul canale interno di replica.
