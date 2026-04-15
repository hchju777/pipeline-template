from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from kernel.exceptions import InputValidationError


def get_dataframe_input(
    inputs: dict[str, Any],
    alias: str,
    plugin_name: str,
) -> pd.DataFrame:
    if alias not in inputs:
        raise InputValidationError(
            f"Plugin '{plugin_name}' missing input alias '{alias}'"
        )

    value = inputs[alias]
    if not isinstance(value, pd.DataFrame):
        raise InputValidationError(
            f"Plugin '{plugin_name}' expected pandas DataFrame for input '{alias}'"
        )

    return value.copy(deep=True)


def require_columns(
    df: pd.DataFrame,
    columns: Iterable[str],
    plugin_name: str,
) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise InputValidationError(
            f"Plugin '{plugin_name}' missing required columns: {missing}"
        )
