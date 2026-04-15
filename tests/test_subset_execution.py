from kernel.cache import CustomTTLCache
from kernel.context import PipelineContext
from kernel.dag_kernel import DAGPipelineKernel
from kernel.loader import load_pipeline_from_json, validate_pipeline_definition
from kernel.plugin_auto_loader import auto_register_plugins
from kernel.registry import PluginRegistry
from kernel.schema_registry import SchemaRegistry
from kernel.validation import ValidationMode
from schemas import register_all_schemas


def _build_kernel():
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
        validation_mode=ValidationMode.BOUNDARY_ONLY,
    )
    return kernel, pipeline


def test_subset_by_tag_includes_upstream_not_downstream():
    kernel, pipeline = _build_kernel()
    context = PipelineContext()
    results = kernel.run(pipeline, context=context, tags=["join"], include_downstream=False)

    assert "merge_ac" in results
    assert "a_lower_name" in results
    assert "collect_a" in results
    assert "collect_c" in results
    assert "output" not in results


def test_subset_by_tag_with_downstream():
    kernel, pipeline = _build_kernel()
    context = PipelineContext()
    results = kernel.run(pipeline, context=context, tags=["join"], include_downstream=True)

    assert "merge_ac" in results
    assert "collect_b" in results
    assert "filter_with_b" in results
    assert "output" in results
    assert [record.node_name for record in context.execution_records] == [
        "collect_a",
        "collect_b",
        "collect_c",
        "a_trim_name",
        "c_joinable",
        "a_lower_name",
        "merge_ac",
        "filter_with_b",
        "output",
    ]
