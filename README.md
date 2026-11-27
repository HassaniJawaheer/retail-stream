# RetailStream

RetailStream est un projet de Data Engineering consacré aux événements e-commerce et supply chain. Un catalogue PostgreSQL alimente une simulation de commandes, réservations et réapprovisionnements. Kafka transporte les événements ; Spark les valide et produit les indicateurs consultables dans Streamlit.

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
uv run --locked retail-ops init
```

Lancer chaque traitement dans un terminal distinct :

| Traitement | Commande |
|---|---|
| Publication Kafka | `uv run --locked retail-outbox` |
| Validation Spark | `uv run --locked retail-stream` |
| Analyse Spark | `uv run --locked retail-business` |
| Expéditions, expirations et réceptions | `uv run --locked retail-ops worker` |

Puis générer des commandes :

```bash
uv run --locked retail-ops simulate --count 20 --rate 2
```

Pour ouvrir le dashboard dans un autre terminal :

```bash
uv run --locked streamlit run src/retail_stream/dashboard.py --server.address 127.0.0.1
```

Ouvrir http://127.0.0.1:8501 et sélectionner **Activité métier** : commandes, stocks et réapprovisionnements, actualisés toutes les dix secondes.

Consulter les résultats dans PostgreSQL :

```bash
docker compose exec postgres psql -U retail -d retail -c "SELECT * FROM business_metrics ORDER BY window_start DESC LIMIT 20;"
```

Pour arrêter : `Ctrl+C` dans les terminaux des traitements, puis `docker compose stop`.

## Tests

```bash
uv run --locked pytest
uv run --locked pytest -m integration
```

## Événements

Les événements décrivent les commandes, réservations, refus, expéditions et mouvements de stock. Le catalogue initial contient 16 produits fictifs ; Best Buy sera intégré ultérieurement.

Le [guide catalogue et stocks](docs/catalogue-et-stocks.md) décrit le contrat produit, les garanties de reprise, les essais manuels et le parcours historique.


## Configuration et stockage

| Variable | Valeur par défaut |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` |
| `KAFKA_ORDERS_TOPIC` | `retail.orders.raw` |
| `KAFKA_VALID_TOPIC` | `retail.orders.valid` |
| `KAFKA_INVALID_TOPIC` | `retail.orders.invalid` |
| `SPARK_BUSINESS_VALIDATION_CHECKPOINT` | `.state/checkpoints/orders-validation-v3` |
| `KAFKA_UNIQUE_TOPIC` | `retail.orders.unique` |
| `SPARK_DEDUP_CHECKPOINT` | `.state/checkpoints/orders-dedup-v1` |
| `SPARK_DEDUP_WATERMARK` | `2 minutes` |
| `DATABASE_URL` | `postgresql://retail:retail_local@127.0.0.1:5433/retail` |
| `SPARK_ANALYTICS_PATH` | `.state/analytics` |
| `SPARK_AGG_WATERMARK` | `2 minutes` |
| `KAFKA_INVENTORY_TOPIC` | `retail.inventory.events` |
| `KAFKA_INVENTORY_INVALID_TOPIC` | `retail.inventory.invalid` |
| `SPARK_BUSINESS_CHECKPOINT` | `.state/checkpoints/business-v1` |

Conserver les volumes PostgreSQL/Kafka et les checkpoints pour la reprise. L'outbox et le journal analytique n'ont pas encore de purge automatique.
