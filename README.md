# Project_Distributed_Systems_Architecture

# Proposte di Homework: KV Store e Contratti Distribuiti

Questo documento raccoglie cinque possibili homework di approfondimento sul
percorso KV store.

Ogni homework parte dagli argomenti visti a lezione, ma chiede di fare un passo
in piu': definire un contratto, implementarlo, difenderlo con test e discuterne
le proprieta' di safety e liveness.

## Organizzazione dei gruppi

Il lavoro e' pensato per gruppi da 3-4 persone.

Ruoli suggeriti:

| Ruolo | Responsabilita' |
| --- | --- |
| Protocol owner | Definisce interfaccia, risposte, precondizioni e casi fuori contratto. |
| Implementation owner | Coordina codice, integrazione e coerenza con lo stile dei lab. |
| Fault/test owner | Costruisce test, scenari di guasto, interleaving e stress. |
| Reviewer/architect | Verifica coerenza tra contratto, implementazione, test e limiti dichiarati. |

Nei gruppi da 3, il ruolo di reviewer puo' essere condiviso.

## Deliverable comuni

Ogni gruppo deve consegnare:

| Deliverable | Contenuto atteso |
| --- | --- |
| Contratto pubblico | Comandi, risposte, precondizioni, postcondizioni e casi fuori contratto. |
| Implementazione | Codice funzionante basato sui laboratori del KV store. |
| Safety/liveness note | Almeno 2 proprieta' di safety e 1 proprieta' di liveness. |
| Test ripetibili | Script o procedura automatizzabile con casi nominali e casi critici. |
| Nota tecnica | Trade-off scelti, limiti rimasti e possibili evoluzioni. |


## Homework 5: Session Consistency Per Client

### Obiettivo

Aggiungere una garanzia di consistenza di sessione.

Un client che ha scritto o letto una certa versione non deve poi osservare, nella
stessa sessione, una versione piu' vecchia della stessa chiave.

Questa garanzia e' piu' debole della linearizzabilita', ma molto utile nei
sistemi distribuiti reali.

### Interfaccia proposta

Esempi:

```text
GETV key SESSION clientA
SET key value SESSION clientA
CAS key version value SESSION clientA
```

Oppure:

```text
GETV key MIN_VERSION 12
```

Il gruppo deve scegliere quale forma rendere pubblica.

### Requisiti minimi

- il sistema deve ricordare o ricevere la versione minima osservata dal client;
- una lettura non deve restituire una versione piu' vecchia della sessione;
- il contratto deve dire se il server aspetta, legge da piu' repliche o risponde con errore;
- i test devono mostrare almeno una lettura che sarebbe stantia senza garanzia di sessione.

### Safety

Proprieta' da discutere:

- read-your-writes per singolo client;
- monotonic reads per singolo client;
- una risposta non deve violare la versione minima dichiarata dalla sessione.

### Liveness

Proprieta' da discutere:

- se nessuna replica aggiornata e' raggiungibile, il sistema non deve bloccarsi indefinitamente senza dichiararlo;
- il client deve poter progredire quando almeno una replica soddisfa la versione minima;
- la gestione della sessione non deve richiedere memoria illimitata sul server.

### Hint

Due strategie ragionevoli:

- sessione stateful: il coordinator ricorda `client_id -> last_seen_version`;
- sessione stateless: il client invia `MIN_VERSION` a ogni richiesta.

La seconda e' spesso piu' semplice da scalare, ma sposta una parte del contratto
sul client.

## Criteri di valutazione suggeriti

| Criterio | Peso indicativo |
| --- | --- |
| Chiarezza del contratto | 25% |
| Correttezza dell'implementazione | 25% |
| Qualita' dei test sui casi critici | 25% |
| Discussione safety/liveness e trade-off | 20% |
| Organizzazione del gruppo e nota tecnica | 5% |

## Indicazione finale

Il lavoro non deve limitarsi ad aggiungere codice.

La domanda principale da difendere e':

> quale promessa nuova introduce il vostro sistema e quale costo tecnico avete
> accettato per mantenerla?

