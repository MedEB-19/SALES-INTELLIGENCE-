"""
COUCHE 4-5 : Traitement + Stockage
- Nettoyage des données
- Calcul des KPIs par région/produit/mois
- Segmentation clients K-Means (simplifié)
- Prévision ventes Random Forest
"""
import os, logging, time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
import json, warnings
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PROCESSING] %(message)s")
log = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://datauser:datapass123@postgres:5432/coca_dwh")
MONGO_URI    = os.getenv("MONGO_URI", "mongodb://datauser:datapass123@mongodb:27017/coca_ml?authSource=admin")

def wait_for_postgres(retries=12, delay=5):
    for i in range(retries):
        try:
            engine = create_engine(POSTGRES_DSN)
            with engine.connect() as c:
                c.execute(text("SELECT 1"))
            log.info("PostgreSQL disponible")
            return engine
        except Exception as e:
            log.warning(f"PostgreSQL indisponible ({e}), tentative {i+1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Echec connexion PostgreSQL")

def wait_for_data(engine, retries=40, delay=10):
    """Attend que des donnees arrivent PUIS que le chargement se stabilise.
    On ne lance le traitement qu'une fois que le nombre de lignes cesse
    d'augmenter (le producer + quality ont fini d'inserer), pour calculer
    les KPIs/previsions sur l'ensemble complet du dataset."""
    last = -1
    stable_checks = 0
    for i in range(retries):
        try:
            with engine.connect() as c:
                count = c.execute(text("SELECT COUNT(*) FROM fait_ventes")).scalar()
        except Exception:
            count = 0

        if count > 0 and count == last:
            stable_checks += 1
            log.info(f"{count} lignes (stable {stable_checks}/2)")
            if stable_checks >= 2:        # 2 controles consecutifs sans hausse
                log.info(f"Chargement termine : {count} lignes dans fait_ventes")
                return count
        else:
            if count > 0:
                log.info(f"{count} lignes chargees, en attente de la fin de l'ingestion...")
            else:
                log.info(f"En attente de donnees... ({i+1}/{retries})")
            stable_checks = 0
        last = count
        time.sleep(delay)

    if last > 0:
        log.warning(f"Delai depasse, traitement sur {last} lignes disponibles")
        return last
    log.warning("Aucune donnee dans fait_ventes apres attente")
    return 0

def load_data(engine) -> pd.DataFrame:
    query = """
        SELECT
            fv.id, fv.date_vente, fv.units_sold, fv.total_sales,
            fv.price_per_unit, fv.operating_profit, fv.operating_margin,
            dr.region_name AS region,
            dp.product_name AS product,
            dp.beverage_brand AS brand,
            drt.retailer_name AS retailer
        FROM fait_ventes fv
        JOIN dim_region dr ON fv.region_id = dr.id
        JOIN dim_produit dp ON fv.produit_id = dp.id
        LEFT JOIN dim_retailer drt ON fv.retailer_id = drt.id
    """
    df = pd.read_sql(query, engine, parse_dates=["date_vente"])
    log.info(f"Donnees chargees : {len(df)} lignes")
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    initial = len(df)
    df = df.dropna(subset=["units_sold", "total_sales", "region"])
    df = df.drop_duplicates()
    df = df[df["units_sold"] >= 0]
    df = df[df["total_sales"] >= 0]
    # Remove outliers IQR
    for col in ["units_sold", "total_sales"]:
        Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
        IQR = Q3 - Q1
        df = df[df[col].between(Q1 - 3 * IQR, Q3 + 3 * IQR)]
    log.info(f"Nettoyage : {initial} -> {len(df)} lignes ({initial-len(df)} supprimees)")
    return df

def compute_kpis(df: pd.DataFrame, engine):
    """Calcule les KPIs et les stocke dans une table dédiée."""
    df["mois"] = df["date_vente"].dt.to_period("M").astype(str)
    kpis = df.groupby(["mois", "region", "product"]).agg(
        ca_total=("total_sales", "sum"),
        profit_total=("operating_profit", "sum"),
        unites_total=("units_sold", "sum"),
        panier_moyen=("price_per_unit", "mean"),
        marge_moyenne=("operating_margin", "mean"),
        nb_transactions=("id", "count"),
    ).reset_index()
    kpis["marge_pct"] = kpis["marge_moyenne"] * 100

    # Créer la table si elle n'existe pas
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kpis_agreg (
                id SERIAL PRIMARY KEY,
                mois VARCHAR(10),
                region VARCHAR(100),
                product VARCHAR(200),
                ca_total NUMERIC(14,2),
                profit_total NUMERIC(14,2),
                unites_total NUMERIC(14,2),
                panier_moyen NUMERIC(10,4),
                marge_pct NUMERIC(6,2),
                nb_transactions INT,
                computed_at TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("DELETE FROM kpis_agreg"))

    kpis[["mois","region","product","ca_total","profit_total","unites_total","panier_moyen","marge_pct","nb_transactions"]]\
        .to_sql("kpis_agreg", engine, if_exists="append", index=False)
    log.info(f"KPIs calcules : {len(kpis)} lignes dans kpis_agreg")

def segment_retailers(df: pd.DataFrame, mongo_client):
    """Segmentation des retailers par K-Means."""
    agg = df.groupby("retailer").agg(
        ca_total=("total_sales", "sum"),
        nb_transactions=("id", "count"),
        marge_moy=("operating_margin", "mean"),
    ).dropna().reset_index()

    if len(agg) < 3:
        log.warning("Pas assez de retailers pour la segmentation")
        return

    features = ["ca_total", "nb_transactions", "marge_moy"]
    scaler = StandardScaler()
    X = scaler.fit_transform(agg[features])
    k = min(3, len(agg))
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    agg["segment"] = km.fit_predict(X)
    agg["segment_label"] = agg["segment"].map({0: "Premium", 1: "Standard", 2: "Basique"})

    records = agg.to_dict(orient="records")
    db = mongo_client["coca_ml"]
    db["segments_retailers"].drop()
    db["segments_retailers"].insert_many(records)
    log.info(f"Segmentation terminee : {len(records)} retailers classes")

def forecast_sales(df: pd.DataFrame, engine):
    """Prévision des ventes par région avec Random Forest."""
    df["month_num"] = df["date_vente"].dt.month
    df["year_num"]  = df["date_vente"].dt.year
    df["day_of_week"] = df["date_vente"].dt.dayofweek

    regions = df["region"].unique()
    results = []

    for region in regions:
        sub = df[df["region"] == region].copy()
        if len(sub) < 10:
            continue
        sub = sub.sort_values("date_vente")
        features = ["month_num", "year_num", "day_of_week"]
        X = sub[features].values
        y = sub["total_sales"].values

        split = max(1, int(len(X) * 0.8))
        X_train, X_test = X[:split], X[split:]
        y_train, y_test = y[:split], y[split:]

        if len(X_train) < 2 or len(X_test) < 1:
            continue

        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))

        # Prévision 3 mois suivants
        last_date = sub["date_vente"].max()
        future_rows = []
        for i in range(1, 4):
            next_m = last_date + pd.DateOffset(months=i)
            future_rows.append([next_m.month, next_m.year, next_m.dayofweek])
        future_pred = model.predict(np.array(future_rows))

        results.append({
            "region": region,
            "rmse": round(rmse, 2),
            "forecast_m1": round(float(future_pred[0]), 2),
            "forecast_m2": round(float(future_pred[1]), 2),
            "forecast_m3": round(float(future_pred[2]), 2),
        })

    if results:
        forecasts_df = pd.DataFrame(results)
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ml_forecasts (
                    id SERIAL PRIMARY KEY,
                    region VARCHAR(100),
                    rmse NUMERIC(14,2),
                    forecast_m1 NUMERIC(14,2),
                    forecast_m2 NUMERIC(14,2),
                    forecast_m3 NUMERIC(14,2),
                    computed_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(text("DELETE FROM ml_forecasts"))
        forecasts_df.to_sql("ml_forecasts", engine, if_exists="append", index=False)
        log.info(f"Previsions calculees pour {len(results)} regions")

def main():
    engine = wait_for_postgres()
    count  = wait_for_data(engine)
    if count == 0:
        log.warning("Aucune donnee a traiter. Verifiez le producer et le validator.")
        return

    df = load_data(engine)
    df = clean_data(df)
    compute_kpis(df, engine)
    forecast_sales(df, engine)

    try:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo.server_info()
        segment_retailers(df, mongo)
    except Exception as e:
        log.warning(f"MongoDB non disponible, segmentation ignoree : {e}")

    log.info("Traitement complet termine avec succes !")

if __name__ == "__main__":
    main()
