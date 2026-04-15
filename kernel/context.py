from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from kernel.models import NodeExecutionRecord


@dataclass
class PipelineContext:
    metadata: dict[str, Any] = field(default_factory=dict)
    logs: list[str] = field(default_factory=list)
    execution_records: list[NodeExecutionRecord] = field(default_factory=list)

    def log(self, message: str) -> None:
        self.logs.append(message)

    def add_record(self, record: NodeExecutionRecord) -> None:
        self.execution_records.append(record)
