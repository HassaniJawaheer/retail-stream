# RetailStream

RetailStream est un projet de Data Engineering consacré aux événements e-commerce et supply chain. Le producteur envoie des commandes fictives à Kafka. Spark les valide et les répartit dans deux topics : `retail.orders.valid` et `retail.orders.invalid`.

## Prérequis

- Python 3.11 et uv ;
- Java 17 ;
- Docker et Docker Compose ;
- ports locaux 9092 et 5433 disponibles.

Versions utilisées : Kafka 4.1.0, PySpark 4.0.1 et confluent-kafka 2.12.0, PostgreSQL 17.6, psycopg 3.2.12 et Streamlit 1.50.0.
Kafka est lancé avec Docker ; les dépendances Python sont installées avec uv.

## Installation et lancement

Depuis la racine du dépôt :

```bash
uv sync --locked
docker compose up -d --wait kafka postgres
docker compose run --rm kafka-init
```

Lancer chaque traitement dans un terminal distinct et le laisser tourner :

| Terminal | Commande |
|---|---|
| 1 — Validation | `uv run --locked retail-stream` |
| 2 — Déduplication | `uv run --locked retail-deduplicate` |
| 3 — Agrégations | `uv run --locked retail-aggregate` |

Dans un quatrième terminal, produire les commandes :

```bash
uv run --locked retail-produce --count 20 --rate 2
```

Pour ouvrir le dashboard dans un autre terminal :

```bash
uv run --locked streamlit run src/retail_stream/dashboard.py --server.address 127.0.0.1
```

Ouvrir http://127.0.0.1:8501 : indicateurs, graphiques et filtres par période et entrepôt, actualisés toutes les dix secondes.

Consulter les résultats dans PostgreSQL :

```bash
docker compose exec postgres psql -U retail -d retail -c "SELECT * FROM order_metrics ORDER BY window_start DESC, warehouse_id LIMIT 20;"
```

Pour arrêter : `Ctrl+C` dans les terminaux des traitements, puis `docker compose stop`.

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
| `KAFKA_UNIQUE_TOPIC` | `retail.orders.unique` |
| `SPARK_DEDUP_CHECKPOINT` | `.state/checkpoints/orders-dedup-v1` |
| `SPARK_DEDUP_WATERMARK` | `2 minutes` |
| `DATABASE_URL` | `postgresql://retail:retail_local@127.0.0.1:5433/retail` |
| `SPARK_ANALYTICS_PATH` | `.state/analytics` |
| `SPARK_AGG_WATERMARK` | `2 minutes` |
