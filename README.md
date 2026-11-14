# RetailStream

RetailStream est un projet de Data Engineering consacré aux événements e-commerce et supply chain. Cette première version fait circuler des commandes fictives de Python vers Kafka, puis les lit avec Spark Structured Streaming.

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

Le producteur affiche les livraisons confirmées par Kafka, avec partition et offset. Spark affiche les commandes dans le premier terminal, par micro-batch toutes les cinq secondes environ.

Pour produire en continu, jusqu'à `Ctrl+C` :

```bash
uv run --locked retail-produce --count 0 --rate 2
```

En alternative au mode continu, après avoir arrêté le premier Spark, lire les messages disponibles puis quitter :

```bash
uv run --locked retail-stream --available-now
```

Spark reprend sa progression grâce au checkpoint : les messages déjà traités ne sont pas réaffichés. Ne pas lancer deux instances Spark avec le même checkpoint.

Pour arrêter les programmes, utiliser `Ctrl+C` dans leurs terminaux, puis arrêter Kafka sans supprimer ses données :

```bash
docker compose stop
```

## Tests

```bash
uv run --locked pytest
uv run --locked pytest -m integration
```

La première commande teste le générateur ; la seconde vérifie le parcours Python → Kafka → Spark et nécessite Kafka démarré.

## Événements

Chaque message JSON représente une création de commande, avec un `event_id`, un `event_type`, une date UTC, un producteur, une version de schéma et un `payload`.

Le contenu métier comporte une commande, un client fictif, un entrepôt, une devise et des articles. Les prix et le montant total sont des entiers en centimes. Le montant est la somme des quantités multipliées par les prix unitaires.

## Configuration et stockage

| Variable | Valeur par défaut |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` |
| `KAFKA_ORDERS_TOPIC` | `retail.orders.raw` |
| `SPARK_CHECKPOINT` | `.state/checkpoints/orders-console` |

Un topic personnalisé doit être créé avant utilisation. Le service `kafka-init` crée uniquement `retail.orders.raw`.

Kafka conserve ses données dans le volume Docker `kafka-data`. La rétention est de 24 heures, avec suppression par segments.

Le broker est accessible uniquement depuis la machine locale. L'installation utilise un seul broker, sans réplication entre machines.