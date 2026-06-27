-- =========================================================================
-- DataMart S.A.S. — Modelo del repositorio analítico
-- Esquema en estrella simple: 2 dimensiones + 1 tabla de hechos unificada
-- (ventas y devoluciones en la misma tabla, diferenciadas por transaction_type)
-- =========================================================================

-- =========================
-- DIMENSIONS
-- =========================

CREATE TABLE IF NOT EXISTS dim_product (
    product_code   TEXT PRIMARY KEY,
    product_name    TEXT,           -- nombre canónico = descripción más frecuente (moda)
    category        TEXT,           -- asignada por heurística de keywords (sin API, ver decisiones)
    country_origin  TEXT,           -- no disponible en los datasets fuente; queda NULL por defecto
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id   DATE PRIMARY KEY,
    year      INT NOT NULL,
    month     INT NOT NULL,
    day       INT NOT NULL,
    week      INT NOT NULL,
    month_name TEXT
);

-- =========================
-- FACT TABLE (unificada: ventas + devoluciones)
-- =========================

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id    BIGSERIAL PRIMARY KEY,
    invoice_no         TEXT NOT NULL,
    product_code       TEXT REFERENCES dim_product(product_code),
    customer_id        TEXT NOT NULL DEFAULT 'UNKNOWN',
    is_identified       BOOLEAN NOT NULL DEFAULT FALSE,
    country             TEXT,
    transaction_type    TEXT NOT NULL CHECK (transaction_type IN ('sale', 'return')),
    quantity             NUMERIC(12,2) NOT NULL,   -- con signo: positivo=sale, negativo=return
    unit_price           NUMERIC(10,2) NOT NULL,
    gross_revenue         NUMERIC(14,2) NOT NULL,   -- quantity * unit_price (con signo)
    transaction_date_utc  TIMESTAMP NOT NULL,
    date_id               DATE REFERENCES dim_date(date_id),
    source_file            TEXT NOT NULL,            -- 'data_csv' | 'online_retail_ii_csv'
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_product_date ON fact_transactions(product_code, date_id);
CREATE INDEX IF NOT EXISTS idx_fact_source ON fact_transactions(source_file);
CREATE INDEX IF NOT EXISTS idx_fact_type ON fact_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_transactions(customer_id);

-- =========================
-- QUALITY / REJECTED DATA
-- =========================

CREATE TABLE IF NOT EXISTS rejected_records (
    id            BIGSERIAL PRIMARY KEY,
    source_file    TEXT NOT NULL,
    invoice_no      TEXT,
    product_code    TEXT,
    reason           TEXT NOT NULL,
    raw_data          JSONB,            -- registro original completo, para trazabilidad
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rejected_source ON rejected_records(source_file);

-- =========================
-- VISTA: revenue neto por producto y día
-- (regla de negocio: bruto de ventas - devoluciones, mismo product_code + fecha)
-- =========================

CREATE OR REPLACE VIEW vw_net_revenue_by_product_day AS
SELECT
    product_code,
    date_id,
    SUM(CASE WHEN transaction_type = 'sale' THEN gross_revenue ELSE 0 END) AS gross_sales_revenue,
    SUM(CASE WHEN transaction_type = 'return' THEN ABS(gross_revenue) ELSE 0 END) AS gross_return_revenue,
    SUM(CASE WHEN transaction_type = 'sale' THEN gross_revenue ELSE 0 END)
        - SUM(CASE WHEN transaction_type = 'return' THEN ABS(gross_revenue) ELSE 0 END) AS net_revenue
FROM fact_transactions
GROUP BY product_code, date_id;