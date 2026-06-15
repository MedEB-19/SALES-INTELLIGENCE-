CREATE TABLE IF NOT EXISTS dim_region (
    id SERIAL PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(100) DEFAULT 'USA',
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS dim_produit (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    beverage_brand VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS dim_retailer (
    id SERIAL PRIMARY KEY,
    retailer_name VARCHAR(200) NOT NULL,
    retailer_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS fait_ventes (
    id SERIAL PRIMARY KEY,
    date_vente DATE NOT NULL,
    region_id INT REFERENCES dim_region(id),
    produit_id INT REFERENCES dim_produit(id),
    retailer_id INT REFERENCES dim_retailer(id),
    units_sold NUMERIC(12,2),
    total_sales NUMERIC(14,2),
    price_per_unit NUMERIC(10,4),
    operating_profit NUMERIC(14,2),
    operating_margin NUMERIC(6,4),
    dq_score INT DEFAULT 100,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS dq_log (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50),
    checked_at TIMESTAMP DEFAULT NOW(),
    total_rows INT,
    passed_rows INT,
    failed_rows INT,
    dq_score NUMERIC(5,2),
    status VARCHAR(20),
    details JSONB
);
CREATE INDEX IF NOT EXISTS idx_ventes_date ON fait_ventes(date_vente);
CREATE INDEX IF NOT EXISTS idx_ventes_region ON fait_ventes(region_id);
CREATE INDEX IF NOT EXISTS idx_ventes_produit ON fait_ventes(produit_id);
CREATE OR REPLACE VIEW vw_ventes_region_mois AS
SELECT
    TO_CHAR(fv.date_vente, 'YYYY-MM') AS mois,
    dr.region_name AS region,
    dp.product_name AS produit,
    dp.beverage_brand AS marque,
    SUM(fv.units_sold) AS total_unites,
    SUM(fv.total_sales) AS total_ca,
    AVG(fv.price_per_unit) AS prix_moyen,
    SUM(fv.operating_profit) AS total_profit,
    AVG(fv.operating_margin) * 100 AS marge_pct
FROM fait_ventes fv
JOIN dim_region dr ON fv.region_id = dr.id
JOIN dim_produit dp ON fv.produit_id = dp.id
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;
CREATE OR REPLACE VIEW vw_kpis_globaux AS
SELECT
    COUNT(*) AS total_transactions,
    SUM(total_sales) AS ca_total,
    SUM(operating_profit) AS profit_total,
    AVG(operating_margin)*100 AS marge_moyenne_pct,
    AVG(units_sold) AS panier_moyen_unites,
    MIN(date_vente) AS date_debut,
    MAX(date_vente) AS date_fin
FROM fait_ventes;
