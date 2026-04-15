from __future__ import annotations

from kernel.exceptions import SchemaRegistryError


class SchemaRegistry:
    def __init__(self) -> None:
        self._schemas: dict[str, type] = {}

    def register(self, name: str, schema_model: type) -> None:
        if not name.strip():
            raise SchemaRegistryError("Schema name must be non-empty")
        if name in self._schemas:
            raise SchemaRegistryError(f"Schema '{name}' already registered")
        self._schemas[name] = schema_model

    def get(self, name: str) -> type:
        schema = self._schemas.get(name)
        if schema is None:
            available = ", ".join(sorted(self._schemas.keys()))
            raise SchemaRegistryError(
                f"Schema '{name}' not found. Available: [{available}]"
            )
        return schema

    def has(self, name: str) -> bool:
        return name in self._schemas

    def list_names(self) -> list[str]:
        return sorted(self._schemas.keys())
