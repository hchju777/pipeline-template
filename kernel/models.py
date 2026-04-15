from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from kernel.validation import ValidationMode


class SchemaRefs(BaseModel):
    inputs: dict[str, str] = Field(default_factory=dict)
    output: str | None = None


class NodeDefinition(BaseModel):
    name: str
    plugin: str
    depends_on: list[str] = Field(default_factory=list)
    inputs: dict[str, str] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)
    schema_refs: SchemaRefs = Field(default_factory=SchemaRefs)
    enabled: bool = True
    on_error: Literal["raise", "skip"] = "raise"
    tags: list[str] = Field(default_factory=list)
    cache: bool = False
    cache_ttl_seconds: int | None = None
    validation_mode: Literal["inherit"] | ValidationMode = "inherit"

    @field_validator("name", "plugin")
    @classmethod
    def validate_non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be a non-empty string")
        return value

    @field_validator("depends_on")
    @classmethod
    def validate_depends_on_unique(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("depends_on contains duplicates")
        return value

    @model_validator(mode="after")
    def validate_cache_fields(self) -> "NodeDefinition":
        if self.cache_ttl_seconds is not None and self.cache_ttl_seconds <= 0:
            raise ValueError("cache_ttl_seconds must be > 0")
        return self


class PipelineDefinition(BaseModel):
    version: str
    pipeline_name: str
    nodes: list[NodeDefinition]

    @field_validator("version", "pipeline_name")
    @classmethod
    def validate_non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be a non-empty string")
        return value

    @model_validator(mode="after")
    def validate_unique_node_names(self) -> "PipelineDefinition":
        names = [node.name for node in self.nodes]
        if len(names) != len(set(names)):
            raise ValueError("duplicate node names detected")
        return self


class NodeExecutionRecord(BaseModel):
    node_name: str
    plugin_name: str
    status: Literal["running", "success", "failed", "skipped", "cache_hit"]
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: float | None = None
    input_row_counts: dict[str, int | None] = Field(default_factory=dict)
    output_row_count: int | None = None
    row_count: int | None = None
    error_message: str | None = None
    cache_key_prefix: str | None = None
    cache_hit: bool = False
    selected_by_subset: bool = False
    validation_mode: str | None = None
