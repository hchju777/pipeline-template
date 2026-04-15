import pandas as pd

from kernel.cache import CustomTTLCache
from kernel.context import PipelineContext
from kernel.dag_kernel import DAGPipelineKernel
from kernel.interfaces import NodePlugin
from kernel.models import NodeDefinition, PipelineDefinition
from kernel.registry import PluginRegistry
from kernel.schema_registry import SchemaRegistry
from kernel.validation import ValidationMode
from schemas import register_all_schemas


def _build_customer_kernel(validation_mode=ValidationMode.BOUNDARY_ONLY):
    from kernel.loader import load_pipeline_from_json, validate_pipeline_definition
    from kernel.plugin_auto_loader import auto_register_plugins

    plugin_registry = PluginRegistry()
    auto_register_plugins(plugin_registry, root_package="plugins")

    schema_registry = SchemaRegistry()
    register_all_schemas(schema_registry)

    pipeline = load_pipeline_from_json("pipelines/customer_pipeline.json")
    validate_pipeline_definition(pipeline, plugin_registry, schema_registry)

    kernel = DAGPipelineKernel(
        registry=plugin_registry,
        schema_registry=schema_registry,
        cache=CustomTTLCache(),
        validation_mode=validation_mode,
    )
    return kernel, pipeline


def _replace_node(pipeline, replacement):
    return pipeline.model_copy(
        update={
            "nodes": [
                replacement if node.name == replacement.name else node
                for node in pipeline.nodes
            ]
        }
    )


def test_on_error_skip_propagates_to_downstream_nodes():
    kernel, pipeline = _build_customer_kernel()
    broken = next(node for node in pipeline.nodes if node.name == "c_joinable")
    replacement = broken.model_copy(
        update={
            "on_error": "skip",
            "options": {
                "source_key": "source",
                "required_fields": ["id", "missing_score"],
            },
        }
    )
    pipeline = _replace_node(pipeline, replacement)
    context = PipelineContext()

    results = kernel.run(pipeline, context=context)

    assert "c_joinable" not in results
    assert "merge_ac" not in results
    assert "filter_with_b" not in results
    assert "output" not in results

    statuses = {
        record.node_name: record.status
        for record in context.execution_records
    }
    assert statuses["c_joinable"] == "skipped"
    assert statuses["merge_ac"] == "skipped"
    assert statuses["filter_with_b"] == "skipped"
    assert statuses["output"] == "skipped"


def test_skip_records_include_downstream_reason():
    kernel, pipeline = _build_customer_kernel()
    broken = next(node for node in pipeline.nodes if node.name == "c_joinable")
    replacement = broken.model_copy(
        update={
            "on_error": "skip",
            "options": {
                "source_key": "source",
                "required_fields": ["id", "missing_score"],
            },
        }
    )
    pipeline = _replace_node(pipeline, replacement)
    context = PipelineContext()

    kernel.run(pipeline, context=context)

    statuses = {record.node_name: record for record in context.execution_records}
    assert statuses["c_joinable"].status == "skipped"
    assert statuses["merge_ac"].status == "skipped"
    assert statuses["filter_with_b"].status == "skipped"
    assert statuses["output"].status == "skipped"
    assert "upstream node" in statuses["merge_ac"].error_message


class VersionedSourceV1(NodePlugin):
    name = "versioned_source"
    version = "1.0.0"
    calls = 0

    def process(self, inputs, context, **kwargs):
        type(self).calls += 1
        return pd.DataFrame([{"id": 1}])


class VersionedSourceV2(NodePlugin):
    name = "versioned_source"
    version = "2.0.0"
    calls = 0

    def process(self, inputs, context, **kwargs):
        type(self).calls += 1
        return pd.DataFrame([{"id": 2}])


def _build_versioned_pipeline():
    return PipelineDefinition(
        version="1.0",
        pipeline_name="versioned_pipeline",
        nodes=[
            NodeDefinition(
                name="source",
                plugin="versioned_source",
                cache=True,
                cache_ttl_seconds=300,
            )
        ],
    )


def test_cache_key_includes_plugin_version():
    cache = CustomTTLCache()
    pipeline = _build_versioned_pipeline()
    schema_registry = SchemaRegistry()

    VersionedSourceV1.calls = 0
    registry_v1 = PluginRegistry()
    registry_v1.register(VersionedSourceV1)
    kernel_v1 = DAGPipelineKernel(
        registry=registry_v1,
        schema_registry=schema_registry,
        cache=cache,
        validation_mode=ValidationMode.OFF,
    )
    result_v1 = kernel_v1.run(pipeline)

    VersionedSourceV2.calls = 0
    registry_v2 = PluginRegistry()
    registry_v2.register(VersionedSourceV2)
    kernel_v2 = DAGPipelineKernel(
        registry=registry_v2,
        schema_registry=schema_registry,
        cache=cache,
        validation_mode=ValidationMode.OFF,
    )
    result_v2 = kernel_v2.run(pipeline)

    assert VersionedSourceV1.calls == 1
    assert VersionedSourceV2.calls == 1
    assert result_v1["source"].loc[0, "id"] == 1
    assert result_v2["source"].loc[0, "id"] == 2


def test_execution_records_include_observability_fields():
    kernel, pipeline = _build_customer_kernel()
    context = PipelineContext()

    kernel.run(pipeline, context=context)

    record = next(record for record in context.execution_records if record.node_name == "merge_ac")
    assert record.validation_mode == ValidationMode.BOUNDARY_ONLY.value
    assert record.duration_ms is not None
    assert record.input_row_counts == {"left": 4, "right": 3}
    assert record.output_row_count == 3
    assert record.row_count == 3
    assert record.cache_key_prefix is not None
    assert len(record.cache_key_prefix) == 12
