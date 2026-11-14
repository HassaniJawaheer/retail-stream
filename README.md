# RetailStream

RetailStream est un projet de Data Engineering consacré aux événements e-commerce et supply chain. Le producteur envoie des commandes fictives à Kafka. Spark les valide et les répartit dans deux topics : `retail.orders.valid` et `retail.orders.invalid`.

## Prérequis

- Python 3.11 et uv ;
- Java 17 ;
- Docker et Docker Compose ;
- port local 9092 disponible.

Versions utilisées : Kafka 4.1.0, PySpark 4.0.1 et confluent-kafka 2.12.0.
Kafka est lancé avec Docker ; les dépendances Python sont installées avec uv.

## Installation et lancement

Depuis la racine du dépôt (Internet nécessaire au premier lancement) :

```bash
uv sync --locked
docker compose up -d kafka
docker compose run --rm kafka-init
```

Dans un premier terminal, depuis la racine du dépôt, démarrer Spark et le laisser tourner :

```bash
uv run --locked retail-stream
```

Dans un deuxième terminal, depuis le même dossier, produire 20 commandes à raison de deux par seconde :

```bash
uv run --locked retail-produce --count 20 --rate 2
```

```bash
uv run --locked retail-produce --count 6 --scenario mixed
```

Pour voir les rejets dans un troisième terminal :

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic retail.orders.invalid --from-beginning
```

Remplacer `retail.orders.invalid` par `retail.orders.valid` pour voir les événements acceptés. Chaque rejet contient le JSON original, les motifs et ses coordonnées Kafka.

Pour arrêter les programmes, utiliser `Ctrl+C` dans leurs terminaux, puis arrêter Kafka sans supprimer ses données :

```bash
docker compose stop
```

## Tests

```bash
uv run --locked pytest
uv run --locked pytest -m integration
```

## Événements

Chaque message JSON représente une création de commande, avec un `event_id`, un `event_type`, une date UTC, un producteur, une version de schéma et un `payload`.


## Configuration et stockage

| Variable | Valeur par défaut |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` |
| `KAFKA_ORDERS_TOPIC` | `retail.orders.raw` |
| `KAFKA_VALID_TOPIC` | `retail.orders.valid` |
| `KAFKA_INVALID_TOPIC` | `retail.orders.invalid` |
| `SPARK_CHECKPOINT` | `.state/checkpoints/orders-validation-v1` |