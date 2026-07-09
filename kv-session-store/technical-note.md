# Technical Note

La soluzione usa una session consistency stateless.

Il coordinator non conserva una mappa del tipo:

```text
(client, key) -> last_seen_version
```

Invece il client mantiene localmente:

```text
min_versions[key] -> version
```

Ogni lettura versionata diventa:

```text
GETV key MIN_VERSION m
```

Dopo una risposta riuscita:

```text
OK key value VERSION v
```

il client aggiorna:

```text
min_versions[key] = max(min_versions[key], v)
```

La stessa regola vale dopo `SET` e `CAS`, perche' entrambe le operazioni
restituiscono la versione prodotta.

## Trade-off

Vantaggio:

- il server rimane semplice e scalabile;
- non esiste stato di sessione da salvare, replicare o ripulire;
- il contratto e' facile da testare: ogni `OK` di `GETV` deve avere
  `VERSION >= MIN_VERSION`.

Costo:

- il client deve ricordare correttamente la versione minima vista per ogni key;
- se il client perde questa tabella, perde anche la garanzia della propria
  sessione precedente.

Questa garanzia e' piu' debole della linearizzabilita': non impone un ordine
globale unico per tutti i client. Garantisce pero' che un singolo client non
torni indietro nel tempo nella propria sessione.
