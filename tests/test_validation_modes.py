import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.typing import Series

from kernel.cache import CustomTTLCache
from kernel.dag_kernel import DAGPipelineKernel
from kernel.interfaces import NodePlugin
from kernel.models import NodeDefinition, PipelineDefinition, SchemaRefs
from kernel.registry import PluginRegistry
from kernel.schema_registry import SchemaRegistry
from kernel.validation import ValidationMode


class IntIdSchema(pa.DataFrameModel):
    id: Series[int]


class ValidSource(NodePlugin):
    name = "valid_source"
    category = "source"

    def process(self, inputs, context, **kwargs):
        return pd.DataFrame([{"id": 1}])


class InvalidSource(NodePlugin):
    name = "invalid_source"
    category = "source"

    def process(self, inputs, context, **kwargs):
        return pd.DataFrame([{"id": "not-an-int"}])


class InvalidFilter(NodePlugin):
    name = "invalid_filter"
    category = "filter"
    required_input_aliases = {"source"}

    def process(self, inputs, context, **kwargs):
        return pd.DataFrame([{"id": "not-an-int"}])


def _build_kernel(validation_mode):
    plugin_registry = PluginRegistry()
    plugin_registry.register(ValidSource)
    plugin_registry.register(InvalidSource)
    plugin_registry.register(InvalidFilter)

    schema_registry = SchemaRegistry()
    schema_registry.register("int_id", IntIdSchema)

    return DAGPipelineKernel(
        registry=plugin_registry,
        schema_registry=schema_registry,
        cache=CustomTTLCache(),
        validation_mode=validation_mode,
    )


def _filter_pipeline(filter_validation_mode="inherit"):
    return PipelineDefinition(
        version="1.0",
        pipeline_name="validation_pipeline",
        nodes=[
            NodeDefinition(
                name="source",
                plugin="valid_source",
                schema_refs=SchemaRefs(output="int_id"),
            ),
            NodeDefinition(
                name="filter",
                plugin="invalid_filter",
                depends_on=["source"],
                inputs={"source": "source"},
                schema_refs=SchemaRefs(
                    inputs={"source": "int_id"},
                    output="int_id",
                ),
                validation_mode=filter_validation_mode,
            ),
        ],
    )


def _invalid_source_pipeline(source_validation_mode="inherit"):
    return PipelineDefinition(
        version="1.0",
        pipeline_name="invalid_source_pipeline",
        nodes=[
            NodeDefinition(
                name="source",
                plugin="invalid_source",
                schema_refs=SchemaRefs(output="int_id"),
                validation_mode=source_validation_mode,
            )
        ],
    )


def test_boundary_only_skips_filter_output_schema_validation():
    kernel = _build_kernel(ValidationMode.BOUNDARY_ONLY)
    results = kernel.run(_filter_pipeline())

    assert results["filter"].loc[0, "id"] == "not-an-int"


def test_strict_validates_filter_output_schema():
    kernel = _build_kernel(ValidationMode.STRICT)

    with pytest.raises(Exception):
        kernel.run(_filter_pipeline())


def test_node_validation_mode_can_upgrade_filter_to_strict():
    kernel = _build_kernel(ValidationMode.BOUNDARY_ONLY)

    with pytest.raises(Exception):
        kernel.run(_filter_pipeline(filter_validation_mode=ValidationMode.STRICT))


def test_node_validation_mode_can_disable_source_validation():
    kernel = _build_kernel(ValidationMode.STRICT)
    results = kernel.run(
        _invalid_source_pipeline(source_validation_mode=ValidationMode.OFF)
    )

    assert results["source"].loc[0, "id"] == "not-an-int"
