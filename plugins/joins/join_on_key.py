from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, field_validator
from typing import Literal

from kernel.interfaces import NodePlugin
from plugins.validation import get_dataframe_input, require_columns


class JoinOnKeyOptions(BaseModel):
    left_input: str
    right_input: str
    left_key: str
    right_key: str
    join_type: Literal["inner", "left"] = "inner"
    right_prefix: str = ""

    @field_validator("left_input", "right_input", "left_key", "right_key")
    @classmethod
    def non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be non-empty")
        return value


class JoinOnKeyPlugin(NodePlugin):
    name = "join_on_key"
    category = "join"
    option_model = JoinOnKeyOptions
    required_input_aliases = {"left", "right"}

    def process(
        self,
        inputs: dict,
        context,
        left_input: str,
        right_input: str,
        left_key: str,
        right_key: str,
        join_type: str = "inner",
        right_prefix: str = "",
    ) -> pd.DataFrame:
        left_df = get_dataframe_input(inputs, left_input, self.name)
        right_df = get_dataframe_input(inputs, right_input, self.name)
        require_columns(left_df, [left_key], self.name)
        require_columns(right_df, [right_key], self.name)

        if right_prefix:
            rename_map = {
                col: f"{right_prefix}{col}"
                for col in right_df.columns
                if col != right_key
            }
            right_df = right_df.rename(columns=rename_map)

        result = left_df.merge(
            right_df,
            how=join_type,
            left_on=left_key,
            right_on=right_key,
        )

        if right_key in result.columns and right_key != left_key:
            result = result.drop(columns=[right_key])

        return result.reset_index(drop=True)
