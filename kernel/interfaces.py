from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from pydantic import BaseModel

from kernel.exceptions import InputValidationError
from kernel.schema_registry import SchemaRegistry
from kernel.validation import ValidationMode


class EmptyOptions(BaseModel):
    pass


class NodePlugin(ABC):
    name: str = "base"
    version: str = "1.0.0"
    category: str = "generic"

    option_model: type[BaseModel] = EmptyOptions
    required_input_aliases: set[str] = set()

    def validate_inputs(self, inputs: dict[str, Any]) -> None:
        missing = self.required_input_aliases - set(inputs.keys())
        if missing:
            raise InputValidationError(
                f"Plugin '{self.name}' missing required inputs: {sorted(missing)}"
            )

    def validate_options(self, options: dict[str, Any]) -> BaseModel:
        return self.option_model.model_validate(options)

    def should_validate_input(
        self,
        validation_mode: ValidationMode,
    ) -> bool:
        if validation_mode == ValidationMode.OFF:
            return False
        if validation_mode == ValidationMode.STRICT:
            return True
        return self.category in {"source", "join", "sink"}

    def should_validate_output(
        self,
        validation_mode: ValidationMode,
    ) -> bool:
        if validation_mode == ValidationMode.OFF:
            return False
        if validation_mode == ValidationMode.STRICT:
            return True
        return self.category in {"source", "join", "sink"}

    def validate_input_frames(
        self,
        inputs: dict[str, Any],
        schema_registry: SchemaRegistry,
        schema_refs: dict[str, str],
        validation_mode: ValidationMode,
    ) -> dict[str, Any]:
        if not self.should_validate_input(validation_mode):
            return dict(inputs)

        validated = dict(inputs)
        for alias, schema_name in schema_refs.items():
            if alias not in validated:
                continue
            if not isinstance(validated[alias], pd.DataFrame):
                raise InputValidationError(
                    f"Plugin '{self.name}' expected pandas DataFrame for input '{alias}'"
                )
            schema_model = schema_registry.get(schema_name)
            validated[alias] = schema_model.validate(validated[alias])
        return validated

    def validate_output_frame(
        self,
        output: Any,
        schema_registry: SchemaRegistry,
        schema_name: str | None,
        validation_mode: ValidationMode,
    ) -> Any:
        if schema_name is None or not self.should_validate_output(validation_mode):
            return output
        if not isinstance(output, pd.DataFrame):
            raise InputValidationError(
                f"Plugin '{self.name}' expected pandas DataFrame output"
            )
        schema_model = schema_registry.get(schema_name)
        return schema_model.validate(output)

    @abstractmethod
    def process(self, inputs: dict[str, Any], context, **kwargs) -> Any:
        raise NotImplementedError
