import uuid
from pathlib import Path

import pandas as pd

INTERMEDIATE_PATH = Path(__file__).resolve().parent.parent / "data" / "intermediate"


def write_intermediate(df: pd.DataFrame, run_id: str, name: str) -> str:
    run_folder = INTERMEDIATE_PATH / _safe_folder_name(run_id)
    run_folder.mkdir(parents=True, exist_ok=True)

    file_path = run_folder / f"{name}.parquet"
    df.to_parquet(file_path, index=False, engine="pyarrow")

    print(f"[io] wrote {len(df)} rows -> {file_path}")
    return str(file_path)


def read_intermediate(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path, engine="pyarrow")
    print(f"[io] read {len(df)} rows <- {path}")
    return df


def _safe_folder_name(run_id: str) -> str:
    safe = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in run_id)
    return safe or uuid.uuid4().hex