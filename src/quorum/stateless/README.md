# Quorum Stateless

Questo progetto usa un approccio stateless per la session consistency.

Il client conserva localmente la versione massima osservata per ogni chiave e
aggiunge `MIN_VERSION <v>` alle letture successive. Il coordinator non conserva
stato di sessione.

## File

- `coordinator.py`: coordinator con quorum, `MIN_VERSION` e read repair.
- `client.py`: client interattivo che traccia le versioni localmente.
- `replica_node.py`: replica in memoria usata dal quorum.
- `acceptance_test.py`: test end-to-end per lo scenario stateless.

## Esecuzione

Da questa cartella:

```bash
python main.py
python acceptance_test.py
python client.py --port 6420
```

`main.py` avvia automaticamente 3 repliche, il coordinator e poi il client
interattivo. Il client continua a ricevere comandi finche' non inserisci
`QUIT` o interrompi il programma.
