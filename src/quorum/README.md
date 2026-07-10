# Quorum Session Consistency

La cartella e' divisa in due progetti separati:

- `stateless/`: il client mantiene lo stato della sessione e invia
  `MIN_VERSION` al coordinator.
- `stateful/`: il coordinator mantiene lo stato della sessione per
  `SESSION <client_id>`.

Ogni progetto contiene il proprio `coordinator.py`, `client.py`,
`replica_node.py`, `main.py` e `acceptance_test.py`.

Avvio rapido:

```bash
python stateless/main.py
python stateful/main.py
```

Ogni `main.py` avvia repliche e coordinator del rispettivo sottoprogetto, poi
lascia aperto il client interattivo per ricevere comandi.
