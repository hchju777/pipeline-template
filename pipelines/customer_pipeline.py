from __future__ import annotations

from typing import Any

from kernel.models import NodeDefinition, PipelineDefinition, SchemaRefs


DEFAULT_CACHE_TTL_SECONDS = 300


def node(
    name: str,
    plugin: str,
    *,
    inputs: dict[str, str] | None = None,
    options: dict[str, Any] | None = None,
    schema_inputs: dict[str, str] | None = None,
    schema_output: str | None = None,
    tags: list[str] | None = None,
    cache: bool = True,
    cache_ttl_seconds: int | None = DEFAULT_CACHE_TTL_SECONDS,
) -> NodeDefinition:
    inputs = inputs or {}
    depends_on = list(dict.fromkeys(inputs.values()))

    return NodeDefinition(
        name=name,
        plugin=plugin,
        depends_on=depends_on,
        inputs=inputs,
        options=options or {},
        schema_refs=SchemaRefs(
            inputs=schema_inputs or {},
            output=schema_output,
        ),
        tags=tags or [],
        cache=cache,
        cache_ttl_seconds=cache_ttl_seconds if cache else None,
    )


def get_pipeline() -> PipelineDefinition:
    return PipelineDefinition(
        version="1.0",
        pipeline_name="customer_pipeline",
        nodes=[
            node(
                "collect_a",
                "collect_a",
                schema_output="customer_base",
                tags=["source"],
            ),
            node(
                "collect_b",
                "collect_b",
                schema_output="allowed_id_base",
                tags=["source"],
            ),
            node(
                "collect_c",
                "collect_c",
                schema_output="score_base",
                tags=["source"],
            ),
            node(
                "a_trim_name",
                "trim_field",
                inputs={"source": "collect_a"},
                options={
                    "source_key": "source",
                    "field": "name",
                },
                schema_inputs={"source": "customer_base"},
                schema_output="customer_base",
                tags=["normalize"],
            ),
            node(
                "a_lower_name",
                "lowercase_field",
                inputs={"source": "a_trim_name"},
                options={
                    "source_key": "source",
                    "field": "name",
                },
                schema_inputs={"source": "customer_base"},
                schema_output="customer_base",
                tags=["normalize"],
            ),
            node(
                "c_joinable",
                "filter_required_fields",
                inputs={"source": "collect_c"},
                options={
                    "source_key": "source",
                    "required_fields": ["id", "score", "grade"],
                },
                schema_inputs={"source": "score_base"},
                schema_output="score_base",
                tags=["filter"],
            ),
            node(
                "merge_ac",
                "join_on_key",
                inputs={
                    "left": "a_lower_name",
                    "right": "c_joinable",
                },
                options={
                    "left_input": "left",
                    "right_input": "right",
                    "left_key": "id",
                    "right_key": "id",
                    "join_type": "inner",
                    "right_prefix": "c_",
                },
                schema_inputs={
                    "left": "customer_base",
                    "right": "score_base",
                },
                schema_output="joined_ac",
                tags=["join", "report"],
            ),
            node(
                "filter_with_b",
                "filter_by_reference",
                inputs={
                    "main": "merge_ac",
                    "reference": "collect_b",
                },
                options={
                    "main_input": "main",
                    "ref_input": "reference",
                    "main_key": "id",
                    "ref_key": "allowed_id",
                },
                schema_inputs={
                    "main": "joined_ac",
                    "reference": "allowed_id_base",
                },
                schema_output="joined_ac",
                tags=["filter", "report"],
            ),
            node(
                "output",
                "print_result",
                inputs={"final_data": "filter_with_b"},
                options={
                    "source_key": "final_data",
                    "title": "FILTERED MERGED RESULT",
                },
                schema_inputs={"final_data": "joined_ac"},
                schema_output="joined_ac",
                tags=["sink", "report"],
                cache=False,
            ),
        ],
    )


pipeline = get_pipeline()
