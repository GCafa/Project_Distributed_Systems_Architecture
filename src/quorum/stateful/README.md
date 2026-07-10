# Quorum Stateful

Questo progetto usa un approccio stateful per la session consistency.

Il client invia `SESSION <client_id>` con i comandi dati. Il coordinator
mantiene la mappa di sessione lato server e applica automaticamente il vincolo
di versione minima per quel client.

## File

- `coordinator.py`: coordinator con quorum, sessioni lato server e read repair.
- `client.py`: client interattivo che invia `SESSION <client_id>`.
- `replica_node.py`: replica in memoria usata dal quorum.
- `acceptance_test.py`: test end-to-end per lo scenario stateful.

## Esecuzione

Da questa cartella:

```bash
python main.py
python acceptance_test.py
python client.py --port 6430 --client-id clientA
```

`main.py` avvia automaticamente 3 repliche, il coordinator e poi il client
interattivo. Il client continua a ricevere comandi finche' non inserisci
`QUIT` o interrompi il programma. Puoi cambiare sessione con
`python main.py --client-id altroClient`.
