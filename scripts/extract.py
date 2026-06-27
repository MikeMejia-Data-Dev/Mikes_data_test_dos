import pandas as pd
from pathlib import Path

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "raw"

# Mapeo de nombres de columna originales -> esquema interno unificado.
# Cubre las variantes observadas en ambos datasets de Kaggle (algunas columnas
# cambian de nombre entre 'data.csv' y 'online_retail_II.csv', ver documento
# de decisiones técnicas, sección "Unificación de fuentes").
COLUMN_MAP = {
    "InvoiceNo": "invoice_no",
    "Invoice": "invoice_no",
    "StockCode": "product_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "Price": "unit_price",
    "CustomerID": "customer_id",
    "Customer ID": "customer_id",
    "Country": "country",
}

REQUIRED_COLUMNS = [
    "invoice_no", "product_code", "description",
    "quantity", "invoice_date", "unit_price", "customer_id", "country",
]


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    existing_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=existing_map)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df


def extract_daily_sales() -> pd.DataFrame:
    """
    Fuente operacional diaria (data.csv). Encoding latin-1 confirmado.
    """
    file_path = DATA_PATH / "data.csv"
    df = pd.read_csv(file_path, encoding="latin-1")
    df = _rename_columns(df)
    df["source_file"] = "data_csv"
    print(f"[extract] data.csv: {len(df)} filas leídas")
    return df


def extract_historical_transactions() -> pd.DataFrame:
    """
    Historial extendido (online_retail_II.csv). Se asume un único CSV
    (no el .xlsx multi-hoja usado en una iteración anterior del proyecto).

    NOTA: si el archivo real viene en .xlsx con múltiples hojas, ajustar
    aquí para leer con pd.ExcelFile y concatenar las hojas, igual que se
    hizo en la versión anterior del pipeline.
    """
    file_path = DATA_PATH / "online_retail_II.csv"
    if file_path.exists():
        df = pd.read_csv(file_path, encoding="latin-1")
    else:
        # Fallback: si solo está disponible el .xlsx, leer todas sus hojas.
        xlsx_path = DATA_PATH / "online_retail_II.xlsx"
        xls = pd.ExcelFile(xlsx_path)
        sheets = [pd.read_excel(xls, sheet_name=s) for s in xls.sheet_names]
        df = pd.concat(sheets, ignore_index=True)
        print(f"[extract] online_retail_II: usando .xlsx (fallback), {len(xls.sheet_names)} hojas combinadas")

    df = _rename_columns(df)
    df["source_file"] = "online_retail_ii"
    print(f"[extract] online_retail_II: {len(df)} filas leídas")
    return df


def extract_all():
    daily = extract_daily_sales()
    historical = extract_historical_transactions()
    return daily, historical