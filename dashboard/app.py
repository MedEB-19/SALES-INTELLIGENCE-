
import os, logging
from flask import Flask, jsonify, render_template
from sqlalchemy import create_engine, text
logging.basicConfig(level=logging.INFO)
app = Flask(__name__, template_folder="templates")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://datauser:datapass123@postgres:5432/coca_dwh")
engine = None
def get_engine():
    global engine
    if engine is None:
        engine = create_engine(POSTGRES_DSN)
    return engine
def query(sql):
    try:
        with get_engine().connect() as conn:
            result = conn.execute(text(sql))
            return [dict(r._mapping) for r in result]
    except Exception as e:
        logging.error(f"DB error: {e}")
        return []
@app.route("/api/kpis")
def api_kpis():
    rows = query("SELECT * FROM vw_kpis_globaux LIMIT 1")
    if not rows:
        return jsonify({"ca_total": 0, "profit_total": 0, "total_transactions": 0, "marge_moyenne_pct": 0})
    r = rows[0]
    return jsonify({k: (float(v) if v is not None and k not in ("date_debut","date_fin") else str(v) if v else None) for k, v in r.items()})
@app.route("/api/ventes_par_region")
def api_ventes_par_region():
    rows = query("""
        SELECT dr.region_name AS region,
               SUM(fv.total_sales) AS ca,
               SUM(fv.units_sold) AS unites
        FROM fait_ventes fv
        JOIN dim_region dr ON fv.region_id = dr.id
        GROUP BY dr.region_name ORDER BY ca DESC
    """)
    return jsonify([{k: float(v) if isinstance(v, (int, float)) else str(v) for k, v in r.items()} for r in rows])
@app.route("/api/evolution_mensuelle")
def api_evolution_mensuelle():
    rows = query("""
        SELECT TO_CHAR(fv.date_vente, 'YYYY-MM') AS mois,
               dr.region_name AS region,
               SUM(fv.total_sales) AS ca
        FROM fait_ventes fv
        JOIN dim_region dr ON fv.region_id = dr.id
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    return jsonify([{k: float(v) if isinstance(v, (int, float)) else str(v) for k, v in r.items()} for r in rows])
@app.route("/api/top_produits")
def api_top_produits():
    rows = query("""
        SELECT dp.product_name AS product, dp.beverage_brand AS brand,
               SUM(fv.total_sales) AS ca, SUM(fv.units_sold) AS unites
        FROM fait_ventes fv
        JOIN dim_produit dp ON fv.produit_id = dp.id
        GROUP BY 1,2 ORDER BY ca DESC LIMIT 10
    """)
    return jsonify([{k: float(v) if isinstance(v, (int, float)) else str(v) for k, v in r.items()} for r in rows])
@app.route("/api/dq_history")
def api_dq_history():
    rows = query("SELECT batch_id, checked_at::text, dq_score, status, total_rows FROM dq_log ORDER BY checked_at DESC LIMIT 20")
    return jsonify([{k: float(v) if isinstance(v, (int, float)) else str(v) for k, v in r.items()} for r in rows])
@app.route("/api/forecasts")
def api_forecasts():
    rows = query("SELECT region, rmse, forecast_m1, forecast_m2, forecast_m3 FROM ml_forecasts ORDER BY region")
    return jsonify([{k: float(v) if isinstance(v, (int, float)) else str(v) for k, v in r.items()} for r in rows])
@app.route("/api/marge_par_region")
def api_marge_par_region():
    rows = query("""
        SELECT dr.region_name AS region,
               ROUND(AVG(fv.operating_margin)*100, 2) AS marge_pct
        FROM fait_ventes fv
        JOIN dim_region dr ON fv.region_id = dr.id
        GROUP BY dr.region_name ORDER BY marge_pct DESC
    """)
    return jsonify([{k: float(v) if isinstance(v, (int, float)) else str(v) for k, v in r.items()} for r in rows])
@app.route("/")
def index():
    return render_template("dashboard.html")
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

