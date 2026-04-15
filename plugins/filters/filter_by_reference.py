from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, field_validator

from kernel.interfaces import NodePlugin
from plugins.validation import get_dataframe_input, require_columns


class FilterByReferenceOptions(BaseModel):
    main_input: str
    ref_input: str
    main_key: str
    ref_key: str

    @field_validator("main_input", "ref_input", "main_key", "ref_key")
    @classmethod
    def non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be non-empty")
        return value


class FilterByReferencePlugin(NodePlugin):
    name = "filter_by_reference"
    category = "filter"
    option_model = FilterByReferenceOptions
    required_input_aliases = {"main", "reference"}

    def process(
        self,
        inputs: dict,
        context,
        main_input: str,
        ref_input: str,
        main_key: str,
        ref_key: str,
    ) -> pd.DataFrame:
        main_df = get_dataframe_input(inputs, main_input, self.name)
        ref_df = get_dataframe_input(inputs, ref_input, self.name)
        require_columns(main_df, [main_key], self.name)
        require_columns(ref_df, [ref_key], self.name)

        allowed_values = set(ref_df[ref_key].tolist())
        return main_df[main_df[main_key].isin(allowed_values)].reset_index(drop=True)
