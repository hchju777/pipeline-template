from __future__ import annotations

from collections import deque
from datetime import UTC, datetime
from typing import Any

from common.hashing import fingerprint_value, hash_text, stable_json_dumps
from kernel.cache import CustomTTLCache
from kernel.context import PipelineContext
from kernel.exceptions import CycleDetectedError, PluginExecutionError
from kernel.models import NodeDefinition, NodeExecutionRecord, PipelineDefinition
from kernel.registry import PluginRegistry
from kernel.schema_registry import SchemaRegistry
from kernel.validation import ValidationMode


class DAGPipelineKernel:
    def __init__(
        self,
        registry: PluginRegistry,
        schema_registry: SchemaRegistry,
        cache: CustomTTLCache | None = None,
        validation_mode: ValidationMode = ValidationMode.BOUNDARY_ONLY,
    ) -> None:
        self.registry = registry
        self.schema_registry = schema_registry
        self.cache = cache or CustomTTLCache()
        self.validation_mode = validation_mode

    @staticmethod
    def _row_count(value: Any) -> int | None:
        if not hasattr(value, "__len__"):
            return None
        try:
            return len(value)
        except TypeError:
            return None

    @staticmethod
    def _finish_record(
        record: NodeExecutionRecord,
        status: str,
        *,
        output: Any = None,
        error_message: str | None = None,
        cache_hit: bool = False,
    ) -> None:
        record.status = status
        record.finished_at = datetime.now(UTC)
        record.error_message = error_message
        record.cache_hit = cache_hit

        if record.started_at is not None:
            duration = record.finished_at - record.started_at
            record.duration_ms = duration.total_seconds() * 1000

        if output is not None:
            output_row_count = DAGPipelineKernel._row_count(output)
            record.output_row_count = output_row_count
            record.row_count = output_row_count

    def _effective_validation_mode(self, node: NodeDefinition) -> ValidationMode:
        if node.validation_mode == "inherit":
            return self.validation_mode
        return ValidationMode(node.validation_mode)

    def _topological_sort(self, nodes: list[NodeDefinition]) -> list[NodeDefinition]:
        node_map = {node.name: node for node in nodes}
        indegree = {node.name: 0 for node in nodes}
        graph = {node.name: [] for node in nodes}

        for node in nodes:
            for dep in node.depends_on:
                if dep in graph:
                    graph[dep].append(node.name)
                    indegree[node.name] += 1

        queue = deque([name for name, degree in indegree.items() if degree == 0])
        sorted_names: list[str] = []

        while queue:
            current = queue.popleft()
            sorted_names.append(current)

            for nxt in graph[current]:
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)

        if len(sorted_names) != len(nodes):
            raise CycleDetectedError("Cycle detected in pipeline DAG")

        return [node_map[name] for name in sorted_names]

    def _build_cache_key(
        self,
        pipeline: PipelineDefinition,
        node: NodeDefinition,
        plugin_version: str,
        resolved_inputs: dict[str, Any],
        validated_options_dict: dict[str, Any],
        validation_mode: ValidationMode,
    ) -> str:
        input_fingerprints = {
            alias: fingerprint_value(value)
            for alias, value in resolved_inputs.items()
        }
        raw_key = stable_json_dumps(
            {
                "pipeline_name": pipeline.pipeline_name,
                "pipeline_version": pipeline.version,
                "node_name": node.name,
                "plugin": node.plugin,
                "plugin_version": plugin_version,
                "options": validated_options_dict,
                "inputs": input_fingerprints,
                "schema_refs": node.schema_refs.model_dump(),
                "validation_mode": validation_mode.value,
            }
        )
        return hash_text(raw_key)

    def _resolve_subset(
        self,
        pipeline: PipelineDefinition,
        tags: list[str] | None,
        node_names: list[str] | None,
        include_downstream: bool,
    ) -> tuple[list[NodeDefinition], set[str]]:
        enabled_nodes = [node for node in pipeline.nodes if node.enabled]
        node_map = {node.name: node for node in enabled_nodes}
        downstream_map: dict[str, list[str]] = {node.name: [] for node in enabled_nodes}
        for node in enabled_nodes:
            for dep in node.depends_on:
                if dep in downstream_map:
                    downstream_map[dep].append(node.name)

        if not tags and not node_names:
            return enabled_nodes, {node.name for node in enabled_nodes}

        target_names: set[str] = set()

        if tags:
            tag_set = set(tags)
            target_names |= {
                node.name for node in enabled_nodes if tag_set.intersection(node.tags)
            }

        if node_names:
            missing = set(node_names) - set(node_map.keys())
            if missing:
                raise PluginExecutionError(f"Unknown node_names requested: {sorted(missing)}")
            target_names |= set(node_names)

        selected_names: set[str] = set()

        def visit_upstream(name: str) -> None:
            if name in selected_names:
                return
            selected_names.add(name)
            for dep in node_map[name].depends_on:
                if dep in node_map:
                    visit_upstream(dep)

        def visit_downstream(name: str) -> None:
            for child in downstream_map.get(name, []):
                if child in selected_names:
                    continue
                selected_names.add(child)
                visit_downstream(child)

        def ensure_upstream_dependencies(name: str, visited: set[str]) -> None:
            if name in visited:
                return
            visited.add(name)

            for dep in node_map[name].depends_on:
                if dep in node_map:
                    selected_names.add(dep)
                    ensure_upstream_dependencies(dep, visited)

        for name in target_names:
            visit_upstream(name)

        if include_downstream:
            base = list(selected_names)
            for name in base:
                visit_downstream(name)
            visited_upstream: set[str] = set()
            for name in list(selected_names):
                ensure_upstream_dependencies(name, visited_upstream)

        return [node for node in enabled_nodes if node.name in selected_names], target_names

    def run(
        self,
        pipeline: PipelineDefinition,
        context: PipelineContext | None = None,
        tags: list[str] | None = None,
        node_names: list[str] | None = None,
        include_downstream: bool = False,
    ) -> dict[str, Any]:
        if context is None:
            context = PipelineContext()

        selected_nodes, target_names = self._resolve_subset(
            pipeline, tags, node_names, include_downstream
        )
        execution_order = self._topological_sort(selected_nodes)
        results: dict[str, Any] = {}
        skipped_nodes: set[str] = set()

        for node in execution_order:
            effective_validation_mode = self._effective_validation_mode(node)
            record = NodeExecutionRecord(
                node_name=node.name,
                plugin_name=node.plugin,
                status="running",
                started_at=datetime.now(UTC),
                selected_by_subset=node.name in target_names,
                validation_mode=effective_validation_mode.value,
            )
            context.add_record(record)
            context.log(f"[START] node={node.name}, plugin={node.plugin}")

            try:
                skipped_upstream = sorted(
                    dep for dep in node.depends_on if dep in skipped_nodes
                )
                if skipped_upstream:
                    message = (
                        "Skipped because upstream node(s) were skipped: "
                        f"{skipped_upstream}"
                    )
                    skipped_nodes.add(node.name)
                    self._finish_record(
                        record,
                        "skipped",
                        error_message=message,
                    )
                    context.log(
                        f"[SKIP] node={node.name}, upstream_skipped={skipped_upstream}"
                    )
                    continue

                plugin = self.registry.create(node.plugin)

                resolved_inputs: dict[str, Any] = {}
                for input_alias, upstream_node_name in node.inputs.items():
                    if upstream_node_name not in results:
                        raise PluginExecutionError(
                            f"Node '{node.name}' requires upstream result "
                            f"'{upstream_node_name}', but it is not available"
                        )
                    resolved_inputs[input_alias] = results[upstream_node_name]

                record.input_row_counts = {
                    alias: self._row_count(value)
                    for alias, value in resolved_inputs.items()
                }
                plugin.validate_inputs(resolved_inputs)
                validated_options = plugin.validate_options(node.options)
                validated_inputs = plugin.validate_input_frames(
                    resolved_inputs,
                    schema_registry=self.schema_registry,
                    schema_refs=node.schema_refs.inputs,
                    validation_mode=effective_validation_mode,
                )

                cache_key = None
                if node.cache:
                    ttl_seconds = node.cache_ttl_seconds or 300
                    cache_key = self._build_cache_key(
                        pipeline=pipeline,
                        node=node,
                        plugin_version=plugin.version,
                        resolved_inputs=validated_inputs,
                        validated_options_dict=validated_options.model_dump(),
                        validation_mode=effective_validation_mode,
                    )
                    record.cache_key_prefix = cache_key[:12]
                    if self.cache.has(cache_key):
                        cached_result = self.cache.get(cache_key)
                        results[node.name] = cached_result
                        self._finish_record(
                            record,
                            "cache_hit",
                            output=cached_result,
                            cache_hit=True,
                        )
                        context.log(f"[CACHE HIT] node={node.name}, plugin={node.plugin}")
                        continue

                result = plugin.process(
                    validated_inputs,
                    context,
                    **validated_options.model_dump(),
                )
                result = plugin.validate_output_frame(
                    result,
                    schema_registry=self.schema_registry,
                    schema_name=node.schema_refs.output,
                    validation_mode=effective_validation_mode,
                )

                results[node.name] = result

                if node.cache and cache_key is not None:
                    ttl_seconds = node.cache_ttl_seconds or 300
                    self.cache.set(cache_key, result, ttl_seconds=ttl_seconds)

                self._finish_record(record, "success", output=result)
                context.log(f"[DONE] node={node.name}, plugin={node.plugin}, rows={record.row_count}")

            except Exception as exc:
                context.log(f"[ERROR] node={node.name}, plugin={node.plugin}, error={exc}")

                if node.on_error == "skip":
                    skipped_nodes.add(node.name)
                    self._finish_record(
                        record,
                        "skipped",
                        error_message=str(exc),
                    )
                    context.log(f"[SKIP] node={node.name}")
                    continue

                self._finish_record(
                    record,
                    "failed",
                    error_message=str(exc),
                )
                raise

        return results
