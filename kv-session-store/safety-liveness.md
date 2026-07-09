# Safety And Liveness

## Safety

Read-your-writes per singolo client:

- dopo un `SET` riuscito, il client salva la versione restituita;
- la lettura successiva usa quella versione come `MIN_VERSION`;
- quindi il coordinator puo' rispondere `OK` solo con una versione almeno pari.

Monotonic reads per singolo client:

- dopo un `GETV` riuscito, il client aggiorna `min_versions[key]`;
- ogni `GETV` successivo porta quella versione minima;
- una replica piu' vecchia non puo' essere scelta per una risposta `OK`.

Rispetto della versione minima:

- `coordinator.getv(key, min_version)` confronta la versione migliore
  raggiungibile con `min_version`;
- se la migliore e' troppo vecchia, ritorna
  `ERR min_version_unavailable`;
- se ritorna `OK`, la versione restituita e' sempre `>= min_version`.

## Liveness

Se almeno una replica raggiungibile contiene una versione `>= MIN_VERSION`, il
coordinator puo' rispondere con successo.

Se nessuna replica raggiungibile contiene una versione sufficiente, il
coordinator non aspetta indefinitamente: ritorna un errore dichiarato.

La soluzione non conserva stato di sessione lato server. Questo evita cleanup di
sessioni inattive e crescita illimitata di mappe `(client, key)` nel coordinator.
