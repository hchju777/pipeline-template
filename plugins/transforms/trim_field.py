from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, field_validator

from kernel.interfaces import NodePlugin
from plugins.validation import get_dataframe_input, require_columns


class TrimFieldOptions(BaseModel):
    source_key: str
    field: str

    @field_validator("source_key", "field")
    @classmethod
    def non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be non-empty")
        return value


class TrimFieldPlugin(NodePlugin):
    name = "trim_field"
    category = "normalize"
    option_model = TrimFieldOptions
    required_input_aliases = {"source"}

    def process(self, inputs: dict, context, source_key: str, field: str) -> pd.DataFrame:
        df = get_dataframe_input(inputs, source_key, self.name)
        require_columns(df, [field], self.name)
        df[field] = df[field].apply(lambda x: x.strip() if isinstance(x, str) else x)
        return df
