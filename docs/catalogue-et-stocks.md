# Catalogue et stocks

## Contrat produit RetailStream

Le catalogue PostgreSQL est la référence métier. Le format Best Buy ne participe à aucune réservation. L'import futur devra produire des fiches conformes au contrat suivant, puis passer par `save_product` ou la commande `retail-ops product fichier.json`.

| Champ | Règle |
|---|---|
| `sku` | Référence interne immuable, 1 à 64 caractères : lettres majuscules, chiffres, `_`, `-` ; commence par une lettre ou un chiffre |
| `product_id` | UUID calculé à partir du SKU interne ; identique à chaque import du même SKU |
| `name`, `manufacturer`, `model`, `category`, `description` | Textes obligatoires, non vides, sans espaces aux extrémités |
| `price_cents` | Entier entre 1 et 100 000 000 ; jamais un flottant |
| `currency` | `EUR` pour cette phase |
| `status` | `active`, `inactive` ou `discontinued` |
| `source`, `source_ref` | Facultatifs, renseignés ensemble ; paire unique |
| `version`, `updated_at` | Gérés par PostgreSQL et le service lors des modifications |

Une fiche JSON d'entrée contient les champs du contrat sauf `product_id`, `version` et `updated_at`, qui sont calculés. La devise et le statut valent par défaut `EUR` et `active`. Le SKU est indépendant de la référence fournisseur : le futur import devra conserver une correspondance stable entre source, référence source et SKU interne. Changer un nom ou un prix ne change pas l'identité du produit.

Le jeu initial contient 16 produits fictifs déterministes, avec 20 unités chacun. `retail-ops init` ne remet pas les stocks à zéro et ne remplace pas les produits déjà présents. Un nouveau produit ajouté ensuite commence avec zéro unité et un mouvement initial traçable. Il faut le réapprovisionner pour vendre ses unités.

Best Buy sera traité ultérieurement : sélection, dédoublonnage, normalisation et rejet des fiches insuffisantes. Un prix en dollars ne devra pas être simplement renommé EUR : la préparation devra choisir et documenter une politique de prix. Aucun import Best Buy n'est inclus ici.

## Règles métier

Un seul entrepôt : `WH-CORBEIL-01`, Corbeil-Essonnes. La clé du stock conserve l'entrepôt pour permettre une évolution future.

- Stock physique : unités présentes.
- Stock réservé : unités affectées aux commandes acceptées.
- Stock disponible : physique moins réservé.
- Quantité commercialement réservable : disponible moins stock de sécurité, nul par défaut.

Une commande réserve toutes ses lignes dans une transaction, ou elle est refusée sans réserver de ligne. Une quantité insuffisante produit `INSUFFICIENT_STOCK` ; un produit retiré de la vente produit `PRODUCT_NOT_SELLABLE`. Une référence inexistante est une demande mal formée : le service la refuse avant création de commande.

Le service verrouille les produits et les stocks dans l'ordre des SKU. Deux commandes concurrentes ne peuvent donc pas réserver les mêmes dernières unités. Une clé de demande identique avec le même contenu retourne la commande existante ; une réutilisation contradictoire est refusée. Les prix et versions des produits sont conservés dans les lignes de commande.

| Opération | Physique | Réservé |
|---|---|---|
| Réserver Q unités | Inchangé | +Q |
| Expédier | -Q | -Q |
| Annuler ou expirer | Inchangé | -Q |
| Recevoir un réapprovisionnement | +Q | Inchangé |

Les réservations expirent après 120 secondes par défaut. Le worker exécute les actions prévues, les expirations et les réceptions arrivées à échéance. Une expédition demandée après expiration libère la réservation au lieu d'expédier. Une action sur une commande déjà terminée retourne son état sans répéter ses mouvements.

Désactiver ou arrêter un produit conserve ses unités et ses commandes. Les réservations déjà acceptées peuvent encore être expédiées. Un réapprovisionnement déjà demandé peut encore être réceptionné ; aucune nouvelle demande n'est créée pour un produit retiré de la vente. La suppression historique n'est pas proposée par la CLI.

Le réapprovisionnement est manuel par défaut. Le worker peut l'automatiser avec `--auto-replenish` : seuil de 5 unités disponibles, cible de 30, au plus une demande ouverte par produit. La réception simulée survient après 30 secondes. Ces paramètres sont stockés dans `inventory`, pas dans le catalogue.

## Tables et relations

| Tables | Responsabilité |
|---|---|
| `products`, `warehouses` | Référentiels produits et entrepôt |
| `inventory` | Stock courant par produit et entrepôt ; contraintes empêchant les quantités négatives et la surréservation |
| `orders`, `order_items` | Commande et lignes avec prix/version produit figés |
| `reservations`, `reservation_items` | Une réservation par commande acceptée, son expiration et ses quantités |
| `stock_movements` | Journal des variations et quantités après opération, reliées à leur opération métier |
| `replenishments` | Demandes et réceptions |
| `outbox` | Événements à publier, erreurs et confirmations de publication |
| `analytics_events` | Identifiants et contenus déjà appliqués à l'analyse |
| `inventory_projection`, `order_projection` | Derniers états observés via Kafka et Spark |
| `replenishment_projection` | Dates de demande/réception observées et délai calculable |
| `business_metrics`, `movement_metrics` | Agrégats par minute UTC |
| `retail_migrations` | Migrations SQL appliquées et empreintes de leur contenu |

Les tables historiques `order_metrics` et les fichiers Parquet existants sont conservés. Les migrations ajoutent les nouvelles tables ; elles ne réécrivent pas les anciennes commandes.

## Flux Kafka et Spark

1. `retail-ops` lit les produits PostgreSQL et demande une commande.
2. Python/PostgreSQL décide de la réservation. Commande, réservation, mouvements et événements outbox sont validés dans la même transaction.
3. `retail-outbox` lit les événements non publiés et les écrit dans Kafka. Il ne les marque publiés qu'après confirmation Kafka.
4. `retail-stream` lit `retail.orders.raw`, contrôle le contrat et les références PostgreSQL, puis écrit dans `retail.orders.valid` ou `retail.orders.invalid`.
5. `retail-business` lit les commandes valides et les événements de stock, valide les stocks, déduplique durablement et calcule les indicateurs.
6. Streamlit lit les résultats PostgreSQL et signale les différences entre stock opérationnel et projection analytique.

| Topic | Contenu / clé |
|---|---|
| `retail.orders.raw` | Événements de commande, clé `order_id` |
| `retail.orders.valid` | Commandes conformes |
| `retail.orders.invalid` | Rejets techniques avec original et motifs |
| `retail.inventory.events` | Mouvements et réapprovisionnements, clé `warehouse_id:sku` |
| `retail.inventory.invalid` | Rejets détectés par l'analyse métier, normalement des événements de stock |
| `retail.orders.unique` | Sortie de déduplication du parcours historique, conservée |

Kafka conserve trois partitions par topic. La clé maintient les événements d'une même entité dans la même partition. Il n'y a pas d'ordre global entre partitions ni entre topics ; les projections utilisent donc les versions métier, jamais la seule heure de réception.

Les commandes métier portent `version=3` : `order_created`, `order_reserved`, `order_rejected`, `order_cancelled`, `reservation_expired`, `order_shipped`. `order_created` signifie demande enregistrée, pas vente confirmée. Un refus métier est un événement valide : il doit être analysé, pas envoyé dans les rejets techniques.

Les événements de stock portent `version=1` : `inventory_changed`, `replenishment_requested`, `replenishment_received`. Un mouvement contient sa variation et le stock résultant. `entity_version` représente la progression d'une commande ou d'un réapprovisionnement ; `stock_version` celle d'un stock. Ces compteurs sont distincts de la version de structure JSON.

La validation contrôle les références existantes, même si un produit a ensuite été désactivé. Elle ne rejoue pas les décisions de réservation : un changement de statut ne doit pas invalider l'historique.

## Analyse, reprise et garanties

Le nouveau parcours lit directement `orders.valid`, sans passer par la déduplication à watermark de `orders.unique`. Une déduplication bornée pourrait éliminer une expédition ou une réception trop ancienne : ce serait inadapté à la reconstruction du stock.

Dans chaque micro-lot, Spark applique les contrôles puis transmet les événements acceptés à `foreachBatch`. Une transaction PostgreSQL :

1. inscrit chaque nouvel `event_id` dans `analytics_events` ;
2. applique les projections uniquement si leur version progresse ;
3. reçoit les agrégats calculés par Spark, groupés par fenêtres fixes d'une minute sur `event_time` ;
4. valide ensemble le journal de déduplication, les projections et les incréments des indicateurs.

Les fenêtres sont calculées sur le DataFrame statique du micro-lot. PostgreSQL accumule leurs contributions. Il n'y a ni watermark ni état temporel Spark pour ce parcours : un événement ancien peut compléter une minute ancienne. Le parcours historique conserve ses exercices de watermark, sa déduplication et son intermédiaire Parquet.

Si une panne intervient après la transaction analytique mais avant la confirmation du checkpoint Spark, le lot est relu et ses événements sont déjà enregistrés : aucun compteur n'est incrémenté une seconde fois. Un identifiant réutilisé avec un contenu différent arrête le traitement au lieu de masquer la contradiction.

Le transport reste « au moins une fois ». L'outbox ne supprime pas la possibilité d'un doublon Kafka après panne ; elle évite de perdre un événement entre transaction métier et publication. L'idempotence est assurée à l'arrivée dans PostgreSQL. Il ne s'agit pas d'une transaction distribuée Kafka/PostgreSQL.

Les indicateurs distinguent demandes, réservations, refus, annulations, expirations et expéditions, avec montants par type et motif. Additionner les montants de tous les types compterait plusieurs fois une même commande : le montant expédié se lit uniquement sur `order_shipped`.

## Essais manuels

Après le lancement décrit dans le README :

```bash
uv run --locked retail-ops products
uv run --locked retail-ops stock
uv run --locked retail-ops order --request-id essai-1 --item SKU-001:2
uv run --locked retail-ops order --request-id essai-refus --item SKU-001:999
uv run --locked retail-ops replenish SKU-001 10 --request-id livraison-1 --delay 0
```

Rejouer `essai-1` avec les mêmes arguments ne crée pas une deuxième commande. Relever son `order_id` pour utiliser `retail-ops ship ID` ou `retail-ops cancel ID` avant expiration. `retail-ops status SKU-001 inactive` retire le produit de la vente ; `active` le réactive.

Sans processus permanents, on peut avancer par étapes :

```bash
uv run --locked retail-ops worker --once
uv run --locked retail-outbox --once
uv run --locked retail-stream --available-now
uv run --locked retail-business --available-now
```

Le worker ne réceptionne que les demandes échues et n'expire que les réservations arrivées à échéance. Relancer ces étapes après avoir effectué de nouvelles opérations.

```bash
docker compose exec postgres psql -U retail -d retail -c "SELECT * FROM business_metrics ORDER BY window_start DESC;"
docker compose exec postgres psql -U retail -d retail -c "SELECT * FROM inventory_projection ORDER BY sku;"
```

Le parcours historique reste utilisable avec `retail-produce`, `retail-stream`, `retail-deduplicate`, `retail-aggregate`. Ses agrégats prennent seulement les événements de versions 1 et 2. Les anciens checkpoints analytiques et données sont préservés. Le validateur utilise désormais un checkpoint par défaut distinct, `orders-validation-v3` ; la première exécution peut relire les messages encore conservés dans Kafka.

## Limites

Le système est local, avec un broker Kafka et un relais outbox sérialisé. Ce n'est pas une installation hautement disponible. Le relais attend chaque confirmation pour privilégier la lisibilité et l'ordre, au prix du débit.

Les événements sont traités en lots limités à 1 000 offsets Kafka par déclenchement. Le catalogue est relu par lot ; les identifiants acceptés sont conservés côté driver pendant le traitement. Ce choix convient au MVP, pas à un catalogue ou un débit arbitrairement grand.

L'outbox publiée et le journal analytique n'ont pas de purge automatique. Leur suppression sans stratégie de reprise casserait les garanties de rejeu. La purge Parquet existante concerne seulement le parcours historique. Conserver la base, les volumes Kafka et les checkpoints ; une restauration partielle n'est pas une reprise cohérente.

Aucun paiement, transporteur, retour, transfert entre entrepôts, expédition partielle ou commande en attente n'est simulé. Les modifications directes de stock par SQL contournent les mouvements et l'outbox : utiliser les opérations métier. Les erreurs PostgreSQL transitoires ne bénéficient pas encore d'une politique générale de reconnexion/retry ; un processus peut devoir être relancé. Les tests couvrent des pannes injectées et de la concurrence locale, pas une campagne de charge ni la perte physique du disque.
