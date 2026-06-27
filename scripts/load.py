import pandas as pd
from sqlalchemy import text

FACT_COLUMNS = [
    "invoice_no", "product_code", "customer_id", "is_identified", "country",
    "transaction_type", "quantity", "unit_price", "gross_revenue",
    "transaction_date_utc", "date_id", "source_file",
]

REJECTED_COLUMNS = ["source_file", "invoice_no", "product_code", "reason", "raw_data"]


def _select_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df[columns]


def load_dim_date(df: pd.DataFrame, engine):
    """
    Genera dim_date a partir de las fechas únicas presentes en las
    transacciones válidas. Idempotente por construcción: usa
    ON CONFLICT DO NOTHING, ya que el contenido de una fecha nunca cambia.
    """
    dates = pd.to_datetime(df["transaction_date_utc"]).dt.date.dropna().unique()
    if len(dates) == 0:
        return

    rows = []
    for d in dates:
        ts = pd.Timestamp(d)
        rows.append({
            "date_id": d,
            "year": ts.year,
            "month": ts.month,
            "day": ts.day,
            "week": ts.isocalendar()[1],
            "month_name": ts.strftime("%B"),
        })
    dim_date_df = pd.DataFrame(rows)

    with engine.begin() as conn:
        for _, row in dim_date_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_date (date_id, year, month, day, week, month_name)
                    VALUES (:date_id, :year, :month, :day, :week, :month_name)
                    ON CONFLICT (date_id) DO NOTHING
                """),
                row.to_dict(),
            )
    print(f"[load] dim_date: {len(dim_date_df)} fechas verificadas/insertadas")


def load_dim_product(canonical_df: pd.DataFrame, engine):
    """
    Upsert de dim_product: si el product_code ya existe, actualiza nombre
    y categoría (por si la moda cambió con nueva información); si no existe,
    lo inserta. Esto mantiene la idempotencia ante reprocesos.
    """
    with engine.begin() as conn:
        for _, row in canonical_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_product (product_code, product_name, category, active)
                    VALUES (:product_code, :product_name, :category, :active)
                    ON CONFLICT (product_code) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        category = EXCLUDED.category
                """),
                row.to_dict(),
            )
    print(f"[load] dim_product: {len(canonical_df)} productos verificados/insertados")


def load_fact_transactions(df: pd.DataFrame, engine, source_file: str):
    """
    Idempotente por DELETE + INSERT: borra todas las transacciones previas
    de este source_file antes de insertar las nuevas. Correr el DAG dos
    veces el mismo día con los mismos datos produce el mismo resultado.
    """
    df_to_load = _select_columns(df, FACT_COLUMNS)

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM fact_transactions WHERE source_file = :sf"),
            {"sf": source_file},
        )

    if df_to_load.empty:
        print(f"[load] fact_transactions: 0 filas para {source_file} (nada que insertar)")
        return

    df_to_load.to_sql(
        "fact_transactions",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"[load] fact_transactions: {len(df_to_load)} filas cargadas para {source_file}")


def load_rejected(df: pd.DataFrame, engine, source_file: str):
    """Mismo patrón idempotente: delete por source_file + insert."""
    if df.empty:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM rejected_records WHERE source_file = :sf"),
                {"sf": source_file},
            )
        print(f"[load] rejected_records: 0 filas para {source_file}")
        return

    df_to_load = df.copy()
    df_to_load["raw_data"] = df_to_load.apply(
        lambda row: row.drop(labels=["reason"], errors="ignore").to_json(), axis=1
    )
    df_to_load = _select_columns(df_to_load, REJECTED_COLUMNS)

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM rejected_records WHERE source_file = :sf"),
            {"sf": source_file},
        )

    df_to_load.to_sql(
        "rejected_records",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"[load] rejected_records: {len(df_to_load)} filas cargadas para {source_file}")