# 🥤 Coca-Cola — Sales Intelligence Platform

**Pipeline de données end-to-end** : CSV → Kafka → Qualité → DWH PostgreSQL → ML → Dashboard

---

## 📁 Structure du projet

```
coca_data_platform/
├── docker-compose.yml          # Orchestration de tous les services
├── data/
│   └── dataset.csv             # ← VOTRE FICHIER ICI
├── config/
│   └── init_db.sql             # Schéma PostgreSQL (auto-exécuté)
├── producer/                   # Couche 1-2 : Ingestion Kafka
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
├── quality/                    # Couche 3 : Validation DQ
│   ├── quality.py
│   ├── requirements.txt
│   └── Dockerfile
├── processing/                 # Couche 4-5 : Traitement + ML
│   ├── processing.py
│   ├── requirements.txt
│   └── Dockerfile
└── dashboard/                  # Couche 8 : Visualisation Flask
    ├── app.py
    ├── templates/dashboard.html
    ├── requirements.txt
    └── Dockerfile
```

---

## 🚀 Démarrage rapide

### Étape 1 — Prérequis

```bash
# Vérifier Docker et Docker Compose
docker --version          # >= 24
docker compose version    # >= 2.20
```

### Étape 2 — Connecter votre dataset

Copiez votre fichier CSV dans le dossier `data/` et renommez-le `dataset.csv` :

```bash
cp /chemin/vers/votre/fichier.csv ./data/dataset.csv
```

**Colonnes reconnues automatiquement** (dataset Kaggle Coca-Cola standard) :

| Colonne originale    | Nom normalisé     |
|----------------------|-------------------|
| Retailer             | retailer          |
| Invoice Date / Date  | invoice_date      |
| Region               | region            |
| State                | state             |
| City                 | city              |
| Beverage Brand       | beverage_brand    |
| Product              | product           |
| Price Per Unit       | price_per_unit    |
| Units Sold           | units_sold        |
| Total Sales          | total_sales       |
| Operating Profit     | operating_profit  |
| Operating Margin     | operating_margin  |

> ℹ️ Si vos colonnes ont des noms différents, éditez `producer/producer.py` → dictionnaire `COLUMN_MAP`.

### Étape 3 — Lancer le projet

```bash
# Depuis la racine du projet
docker compose up --build
```

> ⏱️ Premier démarrage : 3-5 minutes (téléchargement des images Docker)

### Étape 4 — Accéder aux interfaces

| Service          | URL                        | Description                    |
|------------------|----------------------------|--------------------------------|
| 📊 **Dashboard** | http://localhost:5000       | Interface principale            |
| 🔍 **Kafka UI**  | http://localhost:8080       | Monitoring des topics Kafka    |
| 🗄️ **PostgreSQL**| localhost:5432              | DWH (user: datauser / datapass123) |
| 🍃 **MongoDB**   | localhost:27017             | Profils ML segments            |

---

## 🏗️ Architecture des couches

```
[dataset.csv]
      ↓
[COUCHE 1] Sources de données (CSV, POS, CRM)
      ↓
[COUCHE 2] Apache Kafka (AWS MSK) — 12 partitions, rétention 7j
      ↓
[COUCHE 3] Great Expectations — DQ Score 0-100 (blocage si < 80)
      ↓
[COUCHE 4] Databricks / Spark — Nettoyage IQR, KPIs, Streaming
      ↓
[COUCHE 5] Stockage polyglotte
             ├── PostgreSQL DWH (schéma en étoile)
             └── MongoDB (profils ML)
                    ↓
[COUCHE 6] ML / IA
             ├── Random Forest → Prévision ventes 3 mois
             └── K-Means → Segmentation retailers
                    ↓
[COUCHE 7] Apache Airflow (DAG 06:00) + GitHub Actions CI/CD
                    ↓
[COUCHE 8] Dashboard Power BI / Flask — 5 vues décisionnelles
```

---

## 📊 Le Dashboard

Le dashboard affiche en temps réel (auto-refresh 60s) :

- **KPIs globaux** : CA total, profit, transactions, DQ score moyen
- **Évolution mensuelle** des ventes par région (graphe linéaire)
- **Répartition** CA par région (donut chart)
- **Top 10 produits** par chiffre d'affaires
- **Marge par région** (barres horizontales)
- **Prévisions ML** Random Forest (M+1, M+2, M+3) par région
- **Journal qualité** (DQ Log) avec statut PASSED / BLOCKED

---

## 🔄 Flux de données détaillé

1. **Producer** lit `data/dataset.csv` → normalise les colonnes → envoie dans le topic Kafka `ventes` (100 msg/s)
2. **Quality** consomme Kafka → batch de 100 lignes → calcule DQ Score (6 règles) → si ≥ 80 : insère dans PostgreSQL
3. **Processing** interroge PostgreSQL → nettoyage IQR → calcule KPIs mensuels → Random Forest → K-Means → stocke résultats
4. **Dashboard** expose une API REST → Flask → Chart.js → visualisation en temps réel

---

## 🛠️ Commandes utiles

```bash
# Voir les logs d'un service
docker compose logs -f producer
docker compose logs -f quality
docker compose logs -f dashboard

# Redémarrer un service
docker compose restart processing

# Arrêter proprement
docker compose down

# Repartir de zéro (supprime les volumes)
docker compose down -v
docker compose up --build

# Accéder à PostgreSQL
docker exec -it postgres_dwh psql -U datauser -d coca_dwh

# Requête rapide
docker exec -it postgres_dwh psql -U datauser -d coca_dwh \
  -c "SELECT region_name, SUM(total_sales) FROM fait_ventes fv JOIN dim_region dr ON fv.region_id=dr.id GROUP BY 1 ORDER BY 2 DESC;"
```

---

## ⚙️ Configuration avancée

### Variables d'environnement (docker-compose.yml)

| Variable            | Défaut                          | Description                |
|---------------------|---------------------------------|----------------------------|
| `CSV_PATH`          | `/app/data/dataset.csv`         | Chemin du fichier CSV      |
| `DQ_THRESHOLD`      | `80`                            | Seuil de blocage DQ        |
| `KAFKA_NUM_PARTITIONS` | `12`                         | Partitions Kafka           |
| `POSTGRES_DB`       | `coca_dwh`                      | Base de données            |

### Modifier le seuil DQ

Dans `docker-compose.yml`, service `quality` :
```yaml
environment:
  DQ_THRESHOLD: "75"    # Abaisser à 75 si données moins propres
```

---

## 🐛 Résolution de problèmes

| Symptôme | Solution |
|----------|----------|
| Dashboard vide | Attendre que `quality` et `processing` terminent (~2min) |
| "Fichier introuvable" | Vérifier que `data/dataset.csv` existe |
| Kafka non disponible | `docker compose restart producer` après 30s |
| DQ Score = 0 | Colonnes non reconnues → vérifier `COLUMN_MAP` dans producer.py |
| Prévisions absentes | Pas assez de données temporelles — vérifier colonne `invoice_date` |

---

## 📐 Schéma PostgreSQL

```sql
-- Tables de dimension
dim_region    (id, region_name, country)
dim_produit   (id, product_name, category, beverage_brand)
dim_retailer  (id, retailer_name, retailer_type)

-- Table de faits
fait_ventes   (id, date_vente, region_id, produit_id, retailer_id,
               units_sold, total_sales, price_per_unit,
               operating_profit, operating_margin, dq_score)

-- Tables analytiques
kpis_agreg    (mois, region, product, ca_total, profit_total, ...)
ml_forecasts  (region, rmse, forecast_m1, forecast_m2, forecast_m3)
dq_log        (batch_id, dq_score, status, details)

-- Vues
vw_ventes_region_mois   -- agrégation mensuelle par région
vw_kpis_globaux         -- KPIs synthétiques
```

---

## 👤 Auteur

Projet réalisé dans le cadre d'une formation Data Engineering & IA.
Architecture inspirée des meilleures pratiques cloud-native (AWS, Kafka, Delta Lake, MLflow).
