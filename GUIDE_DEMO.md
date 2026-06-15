# 🥤 Coca-Cola Sales Intelligence Platform — Guide de Démo Complet

Guide pas-à-pas pour lancer le projet et le présenter au professeur, de Docker
jusqu'au dashboard. Suis les étapes dans l'ordre.

---

## ✅ Ce qui a été corrigé / ajouté

| Élément | État |
|--------|------|
| `data/dataset.csv` | **Ajouté** — 3 200 ventes, 5 régions, 6 retailers, 7 marques, 12 mois (2024) |
| Service `processing` | **Corrigé** — il attend maintenant la fin de l'ingestion avant de calculer (KPIs/prévisions sur le dataset complet, pas partiel) |
| Pipeline validé | DQ score = 100 → tous les batches PASSED, CA ≈ 2,2 M$, prévisions calculées pour les 5 régions |

Tu n'as **rien d'autre à modifier**. Un seul `docker compose up` suffit.

---

## 1. Prérequis (à vérifier 1 fois)

```bash
docker --version           # >= 24
docker compose version     # >= 2.20
```

**Si tu es sous Windows / WSL2** (tu as déjà eu des soucis WSL avec Mongo) :
- Ouvre **Docker Desktop → Settings → Resources** et donne au moins
  **6 Go de RAM** (Kafka + Spark + Postgres + Mongo, ça consomme).
- Lance toujours les commandes depuis le dossier du projet **à l'intérieur de
  WSL** (`/home/...`), pas depuis `/mnt/c/...` → c'est beaucoup plus rapide et
  ça évite les erreurs de volumes.

---

## 2. Le dataset

Il est **déjà en place** : `data/dataset.csv`. Tu peux le garder tel quel pour la démo.

**Si tu veux utiliser le vrai dataset Kaggle Coca-Cola à la place :**
1. Mets ton fichier ici : `data/dataset.csv` (remplace celui existant).
2. Les colonnes sont auto-détectées (voir `producer/producer.py` → `COLUMN_MAP`).
3. ⚠️ **Attention au format de `Operating Margin`** : le code fait
   `valeur / 100`. Mon dataset stocke la marge en **pourcentage** (ex. `31.5`
   = 31,5 %). Le Kaggle original la stocke souvent en **décimal** (ex. `0.31`).
   Si tu mets le Kaggle brut, les marges s'afficheront ~100× trop petites.
   → soit tu multiplies la colonne par 100 dans ton CSV, soit tu enlèves le
   `/ 100` dans `quality.py` (fonction `insert_ventes`, dernière ligne du INSERT).

---

## 3. Lancer le projet (LA commande)

Depuis la racine `coca_data_platform/` :

```bash
docker compose up --build
```

> ⏱️ **Premier lancement : 3–6 min** (téléchargement des images + build).
> Les fois suivantes : ~30 s.

**Conseil démo :** lance ça **5 minutes AVANT** que le prof arrive, pour que le
build soit fini et que le dashboard soit déjà plein de données quand tu présentes.

### Ce qui se passe (timeline)
1. `zookeeper` + `kafka` démarrent (~30 s).
2. `postgres` crée le schéma (`init_db.sql`) automatiquement.
3. `producer` lit le CSV et envoie 3 200 messages dans le topic `ventes`.
4. `quality` consomme par batches de 100, calcule le **DQ Score**, et insère
   dans PostgreSQL si score ≥ 80.
5. `processing` attend la fin de l'ingestion, puis calcule **KPIs + Random
   Forest (prévisions) + K-Means (segments retailers)**.
6. `dashboard` sert l'interface sur le port 5000.

Quand tu vois dans les logs `[PROCESSING] Traitement complet termine avec succes !`,
**tout est prêt**.

---

## 4. Les interfaces à ouvrir pour la démo

| Service | URL | À montrer |
|--------|-----|-----------|
| 📊 **Dashboard** | http://localhost:5000 | L'écran principal de la présentation |
| 🔍 **Kafka UI** | http://localhost:8080 | Le topic `ventes`, les messages qui circulent |
| 🗄️ PostgreSQL | `localhost:5432` (datauser / datapass123) | Le DWH (via psql, voir §6) |

---

## 5. 🎤 Script de présentation (mappé sur tes 8 couches)

Présente le projet comme un **pipeline data end-to-end**. Suggestion d'ordre :

1. **Couche 1–2 — Ingestion (Kafka)**
   → Ouvre **Kafka UI** (8080), montre le topic `ventes`, le nombre de
   messages, les partitions. *« Chaque vente est un événement streamé en
   temps réel, comme une vraie architecture event-driven (AWS MSK). »*

2. **Couche 3 — Qualité des données**
   → Dans le dashboard, montre le **Journal Qualité (DQ Log)** : statut
   PASSED, score. *« 6 règles automatiques : nulls, valeurs négatives,
   doublons. En dessous de 80, le batch est bloqué — la donnée sale n'entre
   jamais dans le DWH. »*

3. **Couche 4–5 — Traitement & stockage polyglotte**
   → *« Nettoyage IQR pour enlever les outliers, puis stockage en schéma
   étoile dans PostgreSQL (faits + dimensions) et profils ML dans MongoDB. »*
   → Montre une requête psql (§6) pour prouver que le DWH est rempli.

4. **Couche 6 — ML / IA**
   → Dans le dashboard, montre les **Prévisions** (Random Forest, M+1/M+2/M+3
   par région) et parle de la **segmentation K-Means** des retailers
   (Premium / Standard / Basique).

5. **Couche 7 — Orchestration & CI/CD**
   → Mentionne le `docker-compose.yml` (orchestration), le DAG Airflow prévu
   (06:00) et le workflow GitHub Actions (`.github/workflows/ci.yml`).

6. **Couche 8 — Visualisation**
   → Reste sur le dashboard : KPIs globaux (CA, profit, transactions, marge),
   évolution mensuelle, donut par région, top 10 produits, marge par région.
   Auto-refresh toutes les 60 s.

**Phrase de conclusion :** *« Le tout est conteneurisé : une seule commande
reproduit toute la plateforme sur n'importe quelle machine. »*

---

## 6. Vérifier que ça marche (commandes utiles)

```bash
# Suivre les logs d'un service précis
docker compose logs -f producer
docker compose logs -f quality
docker compose logs -f processing

# Combien de ventes dans le DWH ?
docker exec -it postgres_dwh psql -U datauser -d coca_dwh \
  -c "SELECT COUNT(*) FROM fait_ventes;"

# CA par région
docker exec -it postgres_dwh psql -U datauser -d coca_dwh \
  -c "SELECT dr.region_name, ROUND(SUM(fv.total_sales)) AS ca
      FROM fait_ventes fv JOIN dim_region dr ON fv.region_id=dr.id
      GROUP BY 1 ORDER BY 2 DESC;"

# Les prévisions ML
docker exec -it postgres_dwh psql -U datauser -d coca_dwh \
  -c "SELECT * FROM ml_forecasts;"

# Le journal qualité
docker exec -it postgres_dwh psql -U datauser -d coca_dwh \
  -c "SELECT batch_id, dq_score, status FROM dq_log ORDER BY checked_at DESC LIMIT 10;"
```

---

## 7. 🐛 Résolution de problèmes

| Symptôme | Cause / Solution |
|---------|------------------|
| **Dashboard vide** | `quality`/`processing` pas encore finis. Attends le log `Traitement complet termine`. (~2 min après le up) |
| **Pas de prévisions / KPIs** | Relance juste le processing une fois l'ingestion finie : `docker compose run --rm processing` |
| **Kafka : NoBrokersAvailable** | Le producer a démarré trop tôt. `docker compose restart producer` (il retente seul de toute façon). |
| **DQ Score = 0** | Colonnes non reconnues → vérifie `COLUMN_MAP` dans `producer/producer.py`. |
| **Marges affichées minuscules (0,3 % au lieu de 30 %)** | Tu utilises un CSV où la marge est en décimal. Voir §2. |
| **Conteneur tué / out of memory (WSL)** | Augmente la RAM de Docker Desktop (≥ 6 Go). Voir §1. |
| **Port déjà utilisé (5000, 8080, 5432...)** | Un autre service tourne. Ferme-le, ou change le port gauche dans `docker-compose.yml` (`"5050:5000"`). |
| **« Fichier introuvable »** | `data/dataset.csv` manquant. Vérifie qu'il est bien là. |

---

## 8. Arrêter / repartir de zéro

```bash
# Arrêter proprement (garde les données)
docker compose down

# TOUT effacer (volumes Postgres + Mongo) et recommencer propre
docker compose down -v
docker compose up --build
```

> Avant la vraie démo, fais un `down -v` puis un `up --build` à blanc pour
> être sûr que tout repart de zéro sans erreur.

---

## 9. Checklist express avant de présenter

- [ ] `docker compose up --build` lancé 5 min avant
- [ ] Log `[PROCESSING] Traitement complet termine avec succes !` visible
- [ ] http://localhost:5000 → dashboard plein (KPIs ≠ 0)
- [ ] http://localhost:8080 → topic `ventes` visible
- [ ] Une requête psql prête (ex. CA par région)
- [ ] Script des 8 couches relu (§5)

Bonne démo ! 🚀
