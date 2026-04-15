from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, field_validator

from kernel.interfaces import NodePlugin
from plugins.validation import get_dataframe_input, require_columns


class FilterRequiredFieldsOptions(BaseModel):
    source_key: str
    required_fields: list[str]

    @field_validator("source_key")
    @classmethod
    def non_empty_source_key(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be non-empty")
        return value

    @field_validator("required_fields")
    @classmethod
    def validate_required_fields(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("must contain at least one field")

        normalized = [field.strip() for field in value]
        if any(not field for field in normalized):
            raise ValueError("required fields must be non-empty")
        if len(normalized) != len(set(normalized)):
            raise ValueError("required fields must be unique")

        return normalized


class FilterRequiredFieldsPlugin(NodePlugin):
    name = "filter_required_fields"
    category = "filter"
    option_model = FilterRequiredFieldsOptions
    required_input_aliases = {"source"}

    def process(
        self,
        inputs: dict,
        context,
        source_key: str,
        required_fields: list[str],
    ) -> pd.DataFrame:
        df = get_dataframe_input(inputs, source_key, self.name)
        require_columns(df, required_fields, self.name)
        mask = pd.Series(True, index=df.index)

        for field in required_fields:
            series = df[field]
            mask &= series.notna()
            if series.dtype == object:
                mask &= series.apply(
                    lambda value: not (
                        isinstance(value, str) and value.strip() == ""
                    )
                )

        return df.loc[mask].reset_index(drop=True)
