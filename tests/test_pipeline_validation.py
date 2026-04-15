import pytest

from kernel.exceptions import PipelineDefinitionError
from kernel.loader import load_pipeline_from_json, validate_pipeline_definition
from kernel.plugin_auto_loader import auto_register_plugins
from kernel.registry import PluginRegistry
from kernel.schema_registry import SchemaRegistry
from schemas import register_all_schemas


def _build_registries():
    plugin_registry = PluginRegistry()
    auto_register_plugins(plugin_registry, root_package="plugins")

    schema_registry = SchemaRegistry()
    register_all_schemas(schema_registry)

    return plugin_registry, schema_registry


def _replace_node(pipeline, replacement):
    return pipeline.model_copy(
        update={
            "nodes": [
                replacement if node.name == replacement.name else node
                for node in pipeline.nodes
            ]
        }
    )


def test_validation_rejects_missing_required_input_alias():
    plugin_registry, schema_registry = _build_registries()
    pipeline = load_pipeline_from_json("pipelines/customer_pipeline.json")
    node = next(node for node in pipeline.nodes if node.name == "a_trim_name")
    replacement = node.model_copy(update={"inputs": {}})
    pipeline = _replace_node(pipeline, replacement)

    with pytest.raises(PipelineDefinitionError, match="missing required input aliases"):
        validate_pipeline_definition(pipeline, plugin_registry, schema_registry)


def test_validation_rejects_input_not_declared_as_dependency():
    plugin_registry, schema_registry = _build_registries()
    pipeline = load_pipeline_from_json("pipelines/customer_pipeline.json")
    node = next(node for node in pipeline.nodes if node.name == "a_trim_name")
    replacement = node.model_copy(update={"depends_on": []})
    pipeline = _replace_node(pipeline, replacement)

    with pytest.raises(PipelineDefinitionError, match="not listed in depends_on"):
        validate_pipeline_definition(pipeline, plugin_registry, schema_registry)


def test_validation_rejects_invalid_plugin_options():
    plugin_registry, schema_registry = _build_registries()
    pipeline = load_pipeline_from_json("pipelines/customer_pipeline.json")
    node = next(node for node in pipeline.nodes if node.name == "a_trim_name")
    replacement = node.model_copy(
        update={"options": {"source_key": "source", "field": " "}}
    )
    pipeline = _replace_node(pipeline, replacement)

    with pytest.raises(PipelineDefinitionError, match="invalid options"):
        validate_pipeline_definition(pipeline, plugin_registry, schema_registry)
