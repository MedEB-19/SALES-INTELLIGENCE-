"""
COUCHE 3 : Qualité des données (Great Expectations simplifié)
Consomme le topic Kafka 'ventes', valide chaque batch et écrit les résultats dans PostgreSQL
DQ Score < 80 → le lot est rejeté et loggué
"""
import os, json, uuid, logging, time
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [QUALITY] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC        = "ventes"
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://datauser:datapass123@postgres:5432/coca_dwh")
DQ_THRESHOLD = int(os.getenv("DQ_THRESHOLD", "80"))
BATCH_SIZE   = 100

# ── Règles de validation ──────────────────────────────────
RULES = {
    "no_null_units_sold":    lambda df: df["units_sold"].notna().mean() * 100,
    "no_null_total_sales":   lambda df: df["total_sales"].notna().mean() * 100,
    "no_null_region":        lambda df: df["region"].notna().mean() * 100,
    "positive_sales":        lambda df: (df["total_sales"].fillna(0) >= 0).mean() * 100,
    "positive_units":        lambda df: (df["units_sold"].fillna(0) >= 0).mean() * 100,
    "no_duplicate_rows":     lambda df: (1 - df.duplicated().mean()) * 100,
}

def compute_dq_score(df: pd.DataFrame) -> dict:
    scores = {}
    for rule_name, rule_fn in RULES.items():
        try:
            score = rule_fn(df)
        except Exception:
            score = 0.0
        scores[rule_name] = round(float(score), 2)
    overall = round(sum(scores.values()) / len(scores), 2)
    return {"overall": overall, "details": scores}

def wait_for_kafka(retries=15, delay=5):
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="dq_validator_group",
                consumer_timeout_ms=30000,
            )
            log.info("Connecte a Kafka")
            return consumer
        except NoBrokersAvailable:
            log.warning(f"Kafka indisponible, tentative {i+1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Echec connexion Kafka")

def wait_for_postgres(retries=10, delay=5):
    for i in range(retries):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            log.info("Connecte a PostgreSQL")
            return conn
        except Exception as e:
            log.warning(f"PostgreSQL indisponible ({e}), tentative {i+1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Echec connexion PostgreSQL")

def log_dq_result(conn, batch_id, df, dq_result):
    status = "PASSED" if dq_result["overall"] >= DQ_THRESHOLD else "BLOCKED"
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dq_log (batch_id, total_rows, passed_rows, failed_rows, dq_score, status, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            batch_id, len(df),
            int(len(df) * dq_result["overall"] / 100),
            int(len(df) * (1 - dq_result["overall"] / 100)),
            dq_result["overall"],
            status,
            Json(dq_result["details"])
        ))
    conn.commit()
    return status

def insert_ventes(conn, df: pd.DataFrame):
    """Insère les données validées dans le Data Warehouse."""
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            # UPSERT dimension region
            cur.execute("""
                INSERT INTO dim_region (region_name) VALUES (%s)
                ON CONFLICT (region_name) DO NOTHING
                RETURNING id
            """, (str(row.get("region", "Unknown")),))
            result = cur.fetchone()
            if not result:
                cur.execute("SELECT id FROM dim_region WHERE region_name = %s", (str(row.get("region", "Unknown")),))
                result = cur.fetchone()
            region_id = result[0]

            # UPSERT dimension produit
            product_name = str(row.get("product", row.get("beverage_brand", "Unknown")))
            cur.execute("""
                INSERT INTO dim_produit (product_name, beverage_brand)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (product_name, str(row.get("beverage_brand", ""))))
            result = cur.fetchone()
            if not result:
                cur.execute("SELECT id FROM dim_produit WHERE product_name = %s", (product_name,))
                result = cur.fetchone()
            produit_id = result[0] if result else None

            # UPSERT dimension retailer
            retailer = str(row.get("retailer", "Unknown"))
            cur.execute("""
                INSERT INTO dim_retailer (retailer_name) VALUES (%s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (retailer,))
            result = cur.fetchone()
            if not result:
                cur.execute("SELECT id FROM dim_retailer WHERE retailer_name = %s", (retailer,))
                result = cur.fetchone()
            retailer_id = result[0] if result else None

            # Parse date
            try:
                date_val = pd.to_datetime(row.get("invoice_date", row.get("date", "2023-01-01"))).date()
            except Exception:
                date_val = datetime(2023, 1, 1).date()

            # Insérer dans fait_ventes
            cur.execute("""
                INSERT INTO fait_ventes
                    (date_vente, region_id, produit_id, retailer_id, units_sold,
                     total_sales, price_per_unit, operating_profit, operating_margin)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                date_val, region_id, produit_id, retailer_id,
                float(row.get("units_sold", 0) or 0),
                float(row.get("total_sales", 0) or 0),
                float(row.get("price_per_unit", 0) or 0),
                float(row.get("operating_profit", 0) or 0),
                float(str(row.get("operating_margin", 0) or "0").replace("%", "")) / 100,
            ))
    conn.commit()

def main():
    consumer = wait_for_kafka()
    conn     = wait_for_postgres()

    batch  = []
    b_id   = str(uuid.uuid4())[:8]

    log.info(f"En attente de messages sur le topic '{TOPIC}'...")

    for message in consumer:
        batch.append(message.value)

        if len(batch) >= BATCH_SIZE:
            df = pd.DataFrame(batch)
            dq = compute_dq_score(df)
            status = log_dq_result(conn, b_id, df, dq)
            log.info(f"Batch {b_id} | DQ Score: {dq['overall']} | Status: {status}")

            if status == "PASSED":
                insert_ventes(conn, df)
                log.info(f"Batch {b_id} insere dans le DWH ({len(df)} lignes)")
            else:
                log.warning(f"Batch {b_id} BLOQUE (DQ score {dq['overall']} < {DQ_THRESHOLD})")

            batch = []
            b_id  = str(uuid.uuid4())[:8]

    # Flush dernier batch partiel
    if batch:
        df = pd.DataFrame(batch)
        dq = compute_dq_score(df)
        status = log_dq_result(conn, b_id, df, dq)
        if status == "PASSED":
            insert_ventes(conn, df)
            log.info(f"Dernier batch {b_id} insere ({len(df)} lignes)")

    log.info("Validation terminee.")
    conn.close()

if __name__ == "__main__":
    main()
