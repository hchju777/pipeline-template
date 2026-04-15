from kernel.cache import CustomTTLCache
from kernel.context import PipelineContext
from kernel.dag_kernel import DAGPipelineKernel
from kernel.loader import load_pipeline_from_json, validate_pipeline_definition
from kernel.plugin_auto_loader import auto_register_plugins
from kernel.registry import PluginRegistry
from kernel.schema_registry import SchemaRegistry
from kernel.validation import ValidationMode
from schemas import register_all_schemas


def test_full_pipeline_run():
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
        validation_mode=ValidationMode.STRICT,
    )

    context = PipelineContext()
    results = kernel.run(pipeline, context=context)

    assert "output" in results
    df = results["output"]
    assert list(df.columns) == ["id", "name", "c_score", "c_grade"]
    assert len(df) == 2
