"""
COUCHE 1-2 : Sources + Ingestion Kafka
Lit dataset.csv et envoie chaque ligne dans le topic Kafka 'ventes'
Compatible avec le dataset Coca-Cola Kaggle (colonnes auto-détectées)
"""
import os, json, time, uuid, logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CSV_PATH  = os.getenv("CSV_PATH", "/app/data/dataset.csv")
TOPIC     = os.getenv("TOPIC_VENTES", "ventes")
BATCH_ID  = str(uuid.uuid4())[:8]

# Mapping colonnes Kaggle Coca-Cola → noms normalisés
COLUMN_MAP = {
    "retailer":         "retailer",
    "retailer id":      "retailer_id",
    "invoice date":     "invoice_date",
    "date":             "invoice_date",
    "region":           "region",
    "state":            "state",
    "city":             "city",
    "beverage brand":   "beverage_brand",
    "product":          "product",
    "price per unit":   "price_per_unit",
    "units sold":       "units_sold",
    "total sales":      "total_sales",
    "operating profit": "operating_profit",
    "operating margin": "operating_margin",
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]
    rename = {c: COLUMN_MAP[c] for c in df.columns if c in COLUMN_MAP}
    return df.rename(columns=rename)

def wait_for_kafka(retries=15, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Connecte a Kafka avec succes")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka non disponible, tentative {i+1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter a Kafka apres plusieurs tentatives")

def main():
    log.info(f"Lecture du fichier : {CSV_PATH}")
    if not os.path.exists(CSV_PATH):
        log.error(f"Fichier introuvable : {CSV_PATH}")
        log.error("Copiez votre dataset.csv dans le dossier data/")
        return

    df = pd.read_csv(CSV_PATH)
    df = normalize_columns(df)
    log.info(f"{len(df)} lignes chargees | Colonnes: {list(df.columns)}")

    producer = wait_for_kafka()
    sent = 0

    for _, row in df.iterrows():
        msg = row.to_dict()
        msg["batch_id"] = BATCH_ID
        msg["event_time"] = pd.Timestamp.now().isoformat()
        producer.send(TOPIC, value=msg)
        sent += 1
        if sent % 100 == 0:
            log.info(f"{sent}/{len(df)} messages envoyes...")
        time.sleep(0.01)

    producer.flush()
    log.info(f"TERMINE : {sent} messages envoyes dans le topic '{TOPIC}'")

if __name__ == "__main__":
    main()
