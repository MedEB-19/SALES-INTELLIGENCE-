"""
COUCHE 3 — QUALITÉ DES DONNÉES
Valide le CSV, attribue un DQ Score (0-100).
Bloque le pipeline si DQ Score < 80.
"""
import os, sys, json, logging, time
from datetime import datetime
import pandas as pd
import sqlalchemy as sa

logging.basicConfig(level=logging.INFO, format="%(asctime)s [DQ] %(message)s")
log = logging.getLogger(__name__)

PG_HOST   = os.getenv("POSTGRES_HOST", "postgres")
PG_DB     = os.getenv("POSTGRES_DB",   "coca_dwh")
PG_USER   = os.getenv("POSTGRES_USER", "dataeng")
PG_PASS   = os.getenv("POSTGRES_PASSWORD", "DataPlatform2024!")
CSV_PATH  = "/app/data/dataset.csv"
THRESHOLD = 80


def get_engine():
    return sa.create_engine(
        f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DB}",
        connect_args={"connect_timeout": 10},
    )


def run_validations(df):
    checks = []

    # 1. Pas de lignes complètement vides
    empty_rows = int(df.isnull().all(axis=1).sum())
    checks.append(("no_empty_rows", empty_rows == 0, f"{empty_rows} lignes vides"))

    # 2. Pas de doublons
    dupes = int(df.duplicated().sum())
    checks.append(("no_duplicates", dupes == 0, f"{dupes} doublons"))

    # 3. Volume minimum
    checks.append(("min_rows", len(df) >= 10, f"{len(df)} lignes"))

    # 4. Taux nullité < 30% par colonne
    null_rates = (df.isnull().sum() / len(df) * 100)
    bad_cols = {c: round(v, 1) for c, v in null_rates.items() if v > 30}
    checks.append(("null_rate_ok", len(bad_cols) == 0,
                   f"Colonnes >30% null: {bad_cols}" if bad_cols else "OK"))

    # 5. Valeurs négatives dans colonnes de ventes
    num_cols = df.select_dtypes(include="number").columns
    neg_issues = []
    for col in num_cols:
        if any(k in col.lower() for k in ["sale", "vente", "revenue", "qty", "amount", "ca", "total", "volume", "units"]):
            neg = int((df[col] < 0).sum())
            if neg > 0:
                neg_issues.append(f"{col}: {neg} valeurs négatives")
    checks.append(("positive_values", len(neg_issues) == 0,
                   str(neg_issues) if neg_issues else "OK"))

    return checks


def compute_dq_score(checks):
    passed = sum(1 for _, ok, _ in checks if ok)
    return round((passed / len(checks)) * 100)


def save_report(engine, score, checks):
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("""
                INSERT INTO dq_reports (timestamp, dq_score, passed, details)
                VALUES (:ts, :score, :passed, :details)
            """), {
                "ts":      datetime.now().isoformat(),
                "score":   score,
                "passed":  score >= THRESHOLD,
                "details": json.dumps([{"name": n, "passed": ok, "detail": d}
                                        for n, ok, d in checks]),
            })
            conn.commit()
        log.info("✅ Rapport DQ sauvegardé en base PostgreSQL.")
    except Exception as e:
        log.warning(f"⚠️  Impossible de sauvegarder le rapport : {e}")


def main():
    # Attendre que le CSV soit présent
    for _ in range(10):
        if os.path.exists(CSV_PATH):
            break
        log.info(f"⏳ En attente du fichier {CSV_PATH}...")
        time.sleep(5)

    if not os.path.exists(CSV_PATH):
        log.error(f"❌ CSV introuvable : {CSV_PATH}. Placez dataset.csv dans data/")
        sys.exit(1)

    log.info(f"📂 Chargement : {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    log.info(f"   {len(df)} lignes × {len(df.columns)} colonnes")

    checks = run_validations(df)
    score  = compute_dq_score(checks)

    log.info("─" * 55)
    log.info(f"📊 DQ SCORE : {score}/100  (seuil de blocage = {THRESHOLD})")
    for name, ok, detail in checks:
        log.info(f"   {'✅' if ok else '❌'} {name:<25} {detail}")
    log.info("─" * 55)

    try:
        save_report(get_engine(), score, checks)
    except Exception as e:
        log.warning(f"Base non disponible pour rapport : {e}")

    if score < THRESHOLD:
        log.error(f"🚫 DQ Score {score} < {THRESHOLD}. Pipeline BLOQUÉ.")
        sys.exit(1)
    else:
        log.info(f"🟢 DQ Score {score} ≥ {THRESHOLD}. Pipeline AUTORISÉ ✅")


if __name__ == "__main__":
    main()
