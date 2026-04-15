from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd


def stable_json_dumps(data: Any) -> str:
    return json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fingerprint_dataframe(df: pd.DataFrame) -> str:
    csv_repr = df.sort_index(axis=1).to_csv(index=False)
    return hash_text(csv_repr)


def fingerprint_value(value: Any) -> str:
    if isinstance(value, pd.DataFrame):
        return fingerprint_dataframe(value)
    return hash_text(stable_json_dumps(value))
