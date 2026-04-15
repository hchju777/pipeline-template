# Pipeline Engine Specification (AI-Optimized)

## 1. Purpose
This document is an **implementation-ready specification** for a DAG-based pipeline engine.

It is structured so that an AI agent can:
- Generate full codebase
- Maintain consistency
- Extend functionality safely

---

## 2. Core Concepts

### 2.1 Control Plane vs Data Plane

| Layer | Responsibility | Tool |
|------|--------------|------|
| Control Plane | Pipeline definition, options | Pydantic |
| Data Plane | Data validation (DataFrame) | Pandera |

---

## 3. Core Interfaces

### 3.1 NodePlugin

```python
class NodePlugin(ABC):
    name: str
    option_model: Type[BaseModel]

    required_input_aliases: set[str]

    def validate_inputs(self, inputs: dict[str, Any]) -> None: ...
    def validate_options(self, options: dict[str, Any]) -> BaseModel: ...
    def process(self, inputs: dict[str, Any], context, **kwargs) -> Any: ...
```

---

### 3.2 SchemaRegistry

```python
class SchemaRegistry:
    def register(name: str, schema_model: Type[pa.DataFrameModel]) -> None
    def get(name: str) -> Type[pa.DataFrameModel]
```

---

### 3.3 Cache Interface

```python
class Cache:
    def get(key: str) -> Any
    def set(key: str, value: Any, ttl_seconds: int) -> None
    def has(key: str) -> bool
```

---

## 4. Execution Flow

```
JSON → Pydantic Validation → DAG Build → Subset Selection →
Topological Sort → Execute Node → Validate → Cache → Next Node
```

---

## 5. Subset Execution Rules

Given:
```python
run(tags=["report"])
```

Execution set:
```
target_nodes = nodes_with_tag("report")
execution_nodes = target_nodes + all_upstream_dependencies
```

Constraints:
- Upstream ALWAYS included
- Downstream NOT included by default

---

## 6. Cache Rules

### 6.1 Cache Key

```
key = hash(
    node_name +
    plugin_name +
    options +
    upstream_fingerprints
)
```

### 6.2 Storage

```
cache[key] = (value, expires_at)
```

### 6.3 Safety

ALWAYS copy DataFrame:

```
store: df.copy(deep=True)
load: df.copy(deep=True)
```

---

## 7. Schema Handling

### JSON

```json
{
  "schema_refs": {
    "inputs": {
      "source": "customer_base"
    },
    "output": "customer_base"
  }
}
```

### Python

```
schemas/customer.py
schemas/joined.py
```

---

## 8. Validation Levels

| Mode | Description |
|------|------------|
| strict | full validation |
| boundary_only | only source/join/sink |
| off | no validation |

---

## 9. Plugin Design Rules

- MUST be stateless
- MUST NOT mutate inputs in-place
- MUST return new object
- SHOULD be atomic (single responsibility)

---

## 10. Example Node

```json
{
  "name": "merge_ac",
  "plugin": "join_on_key",
  "depends_on": ["a", "c"],
  "inputs": {
    "left": "a",
    "right": "c"
  },
  "options": {
    "left_key": "id",
    "right_key": "id"
  },
  "cache": true
}
```

---

## 11. Failure Policy

| Mode | Behavior |
|------|--------|
| raise | stop pipeline |
| skip | continue |

---

## 12. Extension Points

Future additions:

- Parallel execution
- Distributed cache
- Retry policies
- Metrics/logging backend

---

## 13. Implementation Checklist

- [ ] Pydantic models
- [ ] SchemaRegistry
- [ ] Plugin base class
- [ ] DAG executor
- [ ] Cache system
- [ ] Subset execution
- [ ] Pandera validation

---

## 14. Summary

This spec guarantees:

- Deterministic execution
- Strong validation
- Scalable plugin system
- Safe caching
- AI-friendly structure

