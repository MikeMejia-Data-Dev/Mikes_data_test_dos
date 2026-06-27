import pandas as pd
from zoneinfo import ZoneInfo

# Las fechas crudas no traen zona horaria explícita. Se asume hora local de
# Reino Unido (origen del negocio en ambos datasets) y se convierten a UTC
# antes de cargar, según regla de negocio de la sección 5 del documento.
SOURCE_TZ = ZoneInfo("Europe/London")

# Heurística de categoría por palabra clave en la descripción, usada como
# sustituto de la API de catálogo (plus opcional no implementado por tiempo).
# Es una aproximación documentada, no una clasificación exacta.
CATEGORY_KEYWORDS = {
    "Electrónica": ["LIGHT", "LAMP", "BATTERY", "CABLE", "RADIO", "CLOCK"],
    "Ropa": ["SCARF", "HAT", "GLOVE", "SOCK", "APRON", "BAG"],
    "Deportes": ["BALL", "GAME", "PLAY", "SPORT"],
    "Papelería": ["CARD", "NOTEBOOK", "PEN", "PAPER", "STICKER", "GIFT TAG"],
    "Hogar": [],  # categoría por defecto si no matchea ninguna palabra clave
}


def _assign_category(description: str) -> str:
    if not isinstance(description, str):
        return "Hogar"
    upper = description.upper()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in upper for kw in keywords):
            return category
    return "Hogar"


def clean_and_classify(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica todas las reglas de negocio sobre una fuente ya extraída con
    el esquema unificado (ver extract.py). Devuelve (válidos, rechazados).
    """
    df = df.copy()

    # --- Normalización de tipos ---
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # --- Normalización de product_code: mayúsculas, sin espacios ---
    df["product_code"] = df["product_code"].astype(str).str.strip().str.upper()

    # --- Cliente: sin customer_id -> UNKNOWN, se incluye en el análisis ---
    # customer_id puede llegar como float (NaN mezclado con ids numéricos),
    # lo que arrastra ".0" al convertir a string. Se limpia explícitamente.
    customer_numeric = pd.to_numeric(df["customer_id"], errors="coerce")
    df["customer_id"] = customer_numeric.apply(
        lambda x: str(int(x)) if pd.notna(x) else None
    )
    df["is_identified"] = df["customer_id"].notna()
    df["customer_id"] = df["customer_id"].fillna("UNKNOWN")

    # --- Fechas: parsear, asumir Europe/London, convertir a UTC ---
    parsed_dates = pd.to_datetime(df["invoice_date"], errors="coerce")
    df["transaction_date_utc"] = parsed_dates.dt.tz_localize(
        SOURCE_TZ, ambiguous="NaT", nonexistent="NaT"
    ).dt.tz_convert("UTC")

    # --- Clasificación venta vs devolución (regla de negocio) ---
    df["transaction_type"] = df["quantity"].apply(
        lambda q: "return" if pd.notna(q) and q <= 0 else "sale"
    )

    # --- Revenue bruto (con signo, preserva dirección de la devolución) ---
    df["gross_revenue"] = df["quantity"] * df["unit_price"]

    # --- Reglas de rechazo ---
    # unit_price <= 0 se rechaza SIEMPRE, sea venta o devolución.
    # quantity nula (no convertible a número) también se rechaza.
    # fecha no parseable también se rechaza (no se puede ubicar en el tiempo).
    reject_mask = (
        df["unit_price"].isna()
        | (df["unit_price"] <= 0)
        | df["quantity"].isna()
        | df["transaction_date_utc"].isna()
    )

    rejected = df[reject_mask].copy()
    rejected["reason"] = rejected.apply(_build_reject_reason, axis=1)

    valid = df[~reject_mask].copy()

    print(f"[transform] {len(valid)} válidos, {len(rejected)} rechazados")
    return valid, rejected


def _build_reject_reason(row) -> str:
    reasons = []
    if pd.isna(row["unit_price"]) or row["unit_price"] <= 0:
        reasons.append("unit_price inválido (<=0 o no numérico)")
    if pd.isna(row["quantity"]):
        reasons.append("quantity no numérico")
    if pd.isna(row["transaction_date_utc"]):
        reasons.append("invoice_date no parseable")
    return "; ".join(reasons) if reasons else "motivo no determinado"


def deduplicate_sources(daily: pd.DataFrame, historical: pd.DataFrame) -> pd.DataFrame:
    """
    Las dos fuentes se solapan en fechas. Se usa una clave compuesta
    (invoice_no, product_code, customer_id, transaction_date_utc) para
    detectar duplicados reales entre fuentes. En caso de coincidencia,
    'data_csv' (fuente operacional diaria) tiene prioridad sobre
    'online_retail_ii' (histórico), según decisión documentada.
    """
    combined = pd.concat([daily, historical], ignore_index=True)

    # sort_values asegura que data_csv quede primero dentro de cada grupo
    # duplicado (orden alfabético: 'data_csv' < 'online_retail_ii').
    combined = combined.sort_values("source_file")

    dedup_keys = ["invoice_no", "product_code", "customer_id", "transaction_date_utc"]
    before = len(combined)
    combined = combined.drop_duplicates(subset=dedup_keys, keep="first")
    after = len(combined)

    print(f"[dedup] {before - after} duplicados eliminados entre fuentes (de {before} a {after})")
    return combined


def assign_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["category"] = df["description"].apply(_assign_category)
    return df


def build_canonical_product_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nombre canónico por product_code = descripción más frecuente (moda),
    tras normalizar a mayúsculas. Devuelve un DataFrame listo para dim_product.
    """
    df = df.copy()
    df["description_upper"] = df["description"].astype(str).str.strip().str.upper()

    canonical = (
        df.groupby("product_code")["description_upper"]
        .agg(lambda s: s.value_counts().idxmax() if len(s.dropna()) > 0 else "UNKNOWN")
        .reset_index()
        .rename(columns={"description_upper": "product_name"})
    )

    # Categoría también por moda (consistente con el nombre canónico elegido)
    category_map = (
        df.groupby("product_code")["category"]
        .agg(lambda s: s.value_counts().idxmax() if len(s.dropna()) > 0 else "Hogar")
        .reset_index()
    )

    canonical = canonical.merge(category_map, on="product_code", how="left")
    canonical["active"] = True
    return canonical