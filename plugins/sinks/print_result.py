from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, field_validator

from kernel.interfaces import NodePlugin
from plugins.validation import get_dataframe_input


class PrintResultOptions(BaseModel):
    source_key: str
    title: str = "FINAL RESULT"

    @field_validator("source_key", "title")
    @classmethod
    def non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be non-empty")
        return value


class PrintResultPlugin(NodePlugin):
    name = "print_result"
    category = "sink"
    option_model = PrintResultOptions
    required_input_aliases = {"final_data"}

    def process(self, inputs: dict, context, source_key: str, title: str) -> pd.DataFrame:
        df = get_dataframe_input(inputs, source_key, self.name)
        print(f"\n=== {title} ===")
        print(df)
        return df
