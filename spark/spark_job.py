"""
COUCHE 4 — TRAITEMENT DISTRIBUÉ (PySpark)
=========================================
Pipeline : CSV → SparkSession → Nettoyage IQR → KPIs → PostgreSQL

Ce job remplace spark_processor.py (qui utilisait pandas).
Ici on utilise de vrais DataFrames Spark distribués :
  - spark.read.csv()        : lecture CSV par Spark
  - approxQuantile()        : calcul IQR distribué
  - groupBy().agg()         : agrégations Spark
  - F.col(), F.sum(), etc.  : API DataFrame Spark

Tables produites dans PostgreSQL :
  • fait_ventes_clean       → données nettoyées (sans outliers)
  • kpi_ventes_region       → KPIs agrégés par région × mois
  • kpi_tendance_temporelle → tendance temporelle globale
"""

import os
import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from sqlalchemy import create_engine, text
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(message)s"
)
log = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://datauser:datapass123@postgres:5432/coca_dwh"
)
CSV_PATH = os.getenv("CSV_PATH", "/app/data/dataset.csv")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Attente PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
def wait_for_postgres(retries: int = 15, delay: int = 5):
    for i in range(retries):
        try:
            engine = create_engine(POSTGRES_DSN)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("PostgreSQL disponible.")
            return engine
        except Exception as exc:
            log.warning(f"PostgreSQL indisponible ({exc}) — tentative {i+1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Impossible de joindre PostgreSQL après plusieurs tentatives.")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Création de la SparkSession (mode local, toutes les CPU dispo)
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("CocaCola-SalesIntelligence")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")   # adapté aux petits volumes
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"SparkSession démarrée  (version {spark.version}, master: local[*])")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# 3. Détection automatique des colonnes du CSV
# ─────────────────────────────────────────────────────────────────────────────
def detect_columns(spark_df) -> dict:
    """Fait correspondre les en-têtes du CSV aux rôles métier attendus."""
    # Normalise : minuscules + espaces → underscores
    normalized = {
        col.lower().strip().replace(" ", "_"): col
        for col in spark_df.columns
    }
    candidates = {
        "region":  ["region", "zone", "area", "state", "province", "territory"],
        "date":    ["invoice_date", "date", "periode", "month", "invoice date"],
        "sales":   ["total_sales", "sales", "revenue", "ventes", "ca", "total sales"],
        "units":   ["units_sold", "units sold", "qty", "quantity", "volume"],
        "profit":  ["operating_profit", "profit", "operating profit"],
        "margin":  ["operating_margin", "margin", "operating margin"],
        "product": ["product", "beverage_brand", "produit", "sku", "brand"],
    }
    mapping = {}
    for role, names in candidates.items():
        for name in names:
            norm = name.lower().replace(" ", "_")
            if norm in normalized:
                mapping[role] = normalized[norm]
                break
    log.info(f"Mapping colonnes : {mapping}")
    return mapping


# ─────────────────────────────────────────────────────────────────────────────
# 4. Nettoyage avec PySpark (dropna, dropDuplicates, filtre, IQR)
# ─────────────────────────────────────────────────────────────────────────────
def clean_data(df, mapping: dict):
    """
    Nettoyage complet via DataFrame Spark :
      1. dropna sur les colonnes critiques
      2. dropDuplicates
      3. Filtre des valeurs négatives
      4. Suppression des outliers par méthode IQR (approxQuantile Spark)
    """
    sales_col  = mapping["sales"]   # ex. "Total Sales"
    region_col = mapping["region"]  # ex. "Region"
    units_col  = mapping["units"]   # ex. "Units Sold"

    initial = df.count()

    # ── Cast numériques explicites ──────────────────────────────────────────
    df = (df
          .withColumn(sales_col, F.col(f"`{sales_col}`").cast(DoubleType()))
          .withColumn(units_col, F.col(f"`{units_col}`").cast(DoubleType())))

    # ── Suppression lignes nulles + doublons ────────────────────────────────
    df = df.dropna(subset=[sales_col, region_col, units_col])
    df = df.dropDuplicates()

    # ── Filtrage valeurs négatives ──────────────────────────────────────────
    df = df.filter(
        (F.col(f"`{sales_col}`") >= 0) &
        (F.col(f"`{units_col}`") >= 0)
    )

    # ── Suppression des outliers via IQR (PySpark approxQuantile) ──────────
    # approxQuantile est un opérateur Spark natif qui calcule les percentiles
    # de manière distribuée sur le cluster, avec une précision contrôlée (0.01).
    quantiles = df.approxQuantile(sales_col, [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr     = q3 - q1
    lower   = q1 - 1.5 * iqr
    upper   = q3 + 1.5 * iqr

    df_clean = df.filter(
        (F.col(f"`{sales_col}`") >= lower) &
        (F.col(f"`{sales_col}`") <= upper)
    )

    final = df_clean.count()
    log.info(
        f"Nettoyage : {initial} → {final} lignes | "
        f"outliers supprimés : {initial - final} | "
        f"IQR=[{lower:.2f}, {upper:.2f}]  (Q1={q1:.2f}, Q3={q3:.2f})"
    )
    return df_clean


# ─────────────────────────────────────────────────────────────────────────────
# 5. Ajout colonne mois (format yyyy-MM)
# ─────────────────────────────────────────────────────────────────────────────
def add_month_column(df, date_col: str):
    """
    Tente plusieurs formats de date (Spark coalesce) pour produire
    une colonne 'mois' normalisée (yyyy-MM).
    """
    df = df.withColumn(
        "_date_parsed",
        F.coalesce(
            F.to_date(F.col(f"`{date_col}`"), "yyyy-MM-dd"),
            F.to_date(F.col(f"`{date_col}`"), "M/d/yyyy"),
            F.to_date(F.col(f"`{date_col}`"), "dd/MM/yyyy"),
            F.to_date(F.col(f"`{date_col}`"), "MM/dd/yyyy"),
        )
    ).withColumn("mois", F.date_format(F.col("_date_parsed"), "yyyy-MM"))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 6. Calcul des KPIs par région × mois (groupBy Spark)
# ─────────────────────────────────────────────────────────────────────────────
def compute_kpi_region(df, region_col: str, sales_col: str):
    """
    groupBy() + agg() Spark :
    Calcule CA total, moyen, min, max, count par région × mois.
    """
    kpi = (
        df.groupBy(region_col, "mois")
        .agg(
            F.round(F.sum(f"`{sales_col}`"),  2).alias("total_sales"),
            F.round(F.avg(f"`{sales_col}`"),  2).alias("avg_sales"),
            F.count("*")                        .alias("count_transactions"),
            F.round(F.min(f"`{sales_col}`"),  2).alias("min_sales"),
            F.round(F.max(f"`{sales_col}`"),  2).alias("max_sales"),
        )
        .orderBy(region_col, "mois")
        .withColumn("computed_at", F.current_timestamp())
    )
    n = kpi.count()
    log.info(f"KPIs par région : {n} groupes (région × mois)")
    return kpi


# ─────────────────────────────────────────────────────────────────────────────
# 7. Tendance temporelle globale
# ─────────────────────────────────────────────────────────────────────────────
def compute_temporal_trend(df, sales_col: str):
    """
    Tendance globale des ventes par mois (tous régions confondues).
    """
    trend = (
        df.groupBy("mois")
        .agg(
            F.round(F.sum(f"`{sales_col}`"), 2).alias("total_sales"),
            F.count("*")                        .alias("count_transactions"),
            F.round(F.avg(f"`{sales_col}`"), 2).alias("avg_sales"),
        )
        .orderBy("mois")
        .withColumn("computed_at", F.current_timestamp())
    )
    n = trend.count()
    log.info(f"Tendance temporelle : {n} périodes")
    return trend


# ─────────────────────────────────────────────────────────────────────────────
# 8. Écriture dans PostgreSQL (bridge Spark → pandas → SQLAlchemy)
# ─────────────────────────────────────────────────────────────────────────────
def write_to_postgres(spark_df, table_name: str, engine):
    """
    Convertit le Spark DataFrame en pandas puis écrit dans PostgreSQL.
    Ce bridge pandas est nécessaire car on n'a pas de driver JDBC dans
    ce setup Docker — en production, on utiliserait spark.write.jdbc().
    """
    pdf = spark_df.toPandas()

    # Convertir les colonnes Timestamp Spark en str pour SQLAlchemy
    for col in pdf.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]).columns:
        pdf[col] = pdf[col].astype(str)

    # Normaliser les noms de colonnes (espaces → underscores)
    pdf.columns = [c.lower().replace(" ", "_") for c in pdf.columns]

    pdf.to_sql(table_name, engine, if_exists="replace",
               index=False, method="multi", chunksize=500)
    log.info(f"  ✓  {len(pdf)} lignes écrites dans '{table_name}'")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  COCA-COLA — JOB SPARK (COUCHE 4 — TRAITEMENT DISTRIBUÉ)")
    log.info("=" * 60)

    # Attendre que PostgreSQL soit prêt
    engine = wait_for_postgres()

    if not os.path.exists(CSV_PATH):
        log.error(f"CSV introuvable : {CSV_PATH}")
        return

    spark = create_spark_session()

    try:
        # ── Lecture CSV avec PySpark ──────────────────────────────────────
        log.info(f"Lecture CSV : {CSV_PATH}")
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(CSV_PATH)
        )
        log.info(f"Données brutes : {df.count()} lignes | colonnes : {df.columns}")
        log.info(f"Schéma Spark inféré :")
        for field in df.schema.fields:
            log.info(f"    {field.name:<30} {field.dataType}")

        # ── Détection des colonnes ────────────────────────────────────────
        mapping = detect_columns(df)

        # ── Nettoyage distribué (IQR via approxQuantile) ─────────────────
        df_clean = clean_data(df, mapping)

        # ── Ajout colonne mois ────────────────────────────────────────────
        df_clean = add_month_column(df_clean, mapping["date"])

        # ── Écriture données nettoyées ────────────────────────────────────
        log.info("Écriture fait_ventes_clean...")
        write_to_postgres(
            df_clean.drop("_date_parsed", "mois"),
            "fait_ventes_clean",
            engine
        )

        # ── KPIs par région × mois ────────────────────────────────────────
        log.info("Calcul KPIs par région...")
        kpi_region = compute_kpi_region(df_clean, mapping["region"], mapping["sales"])
        write_to_postgres(kpi_region, "kpi_ventes_region", engine)

        # ── Tendance temporelle ───────────────────────────────────────────
        log.info("Calcul tendance temporelle...")
        trend = compute_temporal_trend(df_clean, mapping["sales"])
        write_to_postgres(trend, "kpi_tendance_temporelle", engine)

        log.info("=" * 60)
        log.info("  JOB SPARK TERMINÉ AVEC SUCCÈS")
        log.info("  Tables créées dans PostgreSQL :")
        log.info("    → fait_ventes_clean       (données nettoyées)")
        log.info("    → kpi_ventes_region       (KPIs par région × mois)")
        log.info("    → kpi_tendance_temporelle (tendance globale)")
        log.info("=" * 60)

    finally:
        spark.stop()
        log.info("SparkSession arrêtée proprement.")


if __name__ == "__main__":
    main()
    