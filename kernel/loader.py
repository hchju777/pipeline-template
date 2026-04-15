from __future__ import annotations

import importlib.util
from pathlib import Path

from kernel.exceptions import PipelineDefinitionError
from kernel.models import PipelineDefinition
from kernel.registry import PluginRegistry
from kernel.schema_registry import SchemaRegistry


def load_pipeline_from_json(path: str) -> PipelineDefinition:
    try:
        json_text = Path(path).read_text(encoding="utf-8")
        return PipelineDefinition.model_validate_json(json_text)
    except Exception as exc:
        raise PipelineDefinitionError(f"Failed to load pipeline JSON: {exc}") from exc


def load_pipeline_from_python(path: str) -> PipelineDefinition:
    pipeline_path = Path(path).resolve()
    module_name = f"_pipeline_{pipeline_path.stem}_{abs(hash(pipeline_path))}"

    try:
        spec = importlib.util.spec_from_file_location(module_name, pipeline_path)
        if spec is None or spec.loader is None:
            raise PipelineDefinitionError(f"Cannot import Python pipeline: {path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if hasattr(module, "get_pipeline"):
            pipeline = module.get_pipeline()
        elif hasattr(module, "pipeline"):
            pipeline = module.pipeline
        else:
            raise PipelineDefinitionError(
                "Python pipeline must expose get_pipeline() or pipeline"
            )

        return PipelineDefinition.model_validate(pipeline)
    except PipelineDefinitionError:
        raise
    except Exception as exc:
        raise PipelineDefinitionError(
            f"Failed to load pipeline Python file: {exc}"
        ) from exc


def load_pipeline(path: str) -> PipelineDefinition:
    suffix = Path(path).suffix.lower()
    if suffix == ".json":
        return load_pipeline_from_json(path)
    if suffix == ".py":
        return load_pipeline_from_python(path)
    raise PipelineDefinitionError(
        f"Unsupported pipeline file extension '{suffix}'. Use .json or .py"
    )


def validate_pipeline_definition(
    pipeline: PipelineDefinition,
    plugin_registry: PluginRegistry,
    schema_registry: SchemaRegistry,
) -> None:
    node_name_set = {node.name for node in pipeline.nodes}

    for node in pipeline.nodes:
        if not plugin_registry.has(node.plugin):
            raise PipelineDefinitionError(
                f"Node '{node.name}' references unknown plugin '{node.plugin}'"
            )

        plugin = plugin_registry.create(node.plugin)
        missing_aliases = plugin.required_input_aliases - set(node.inputs.keys())
        if missing_aliases:
            raise PipelineDefinitionError(
                f"Node '{node.name}' is missing required input aliases "
                f"for plugin '{node.plugin}': {sorted(missing_aliases)}"
            )

        try:
            plugin.validate_options(node.options)
        except Exception as exc:
            raise PipelineDefinitionError(
                f"Node '{node.name}' has invalid options for plugin "
                f"'{node.plugin}': {exc}"
            ) from exc

        for dep in node.depends_on:
            if dep not in node_name_set:
                raise PipelineDefinitionError(
                    f"Node '{node.name}' depends on unknown node '{dep}'"
                )

        for input_alias, upstream in node.inputs.items():
            if upstream not in node_name_set:
                raise PipelineDefinitionError(
                    f"Node '{node.name}' inputs reference unknown node '{upstream}'"
                )
            if upstream not in node.depends_on:
                raise PipelineDefinitionError(
                    f"Node '{node.name}' input alias '{input_alias}' references "
                    f"node '{upstream}', but '{upstream}' is not listed in depends_on"
                )

        for alias, schema_name in node.schema_refs.inputs.items():
            if alias not in node.inputs:
                raise PipelineDefinitionError(
                    f"Node '{node.name}' input schema ref '{alias}' has no matching input alias"
                )
            if not schema_registry.has(schema_name):
                raise PipelineDefinitionError(
                    f"Node '{node.name}' references unknown input schema '{schema_name}'"
                )

        if node.schema_refs.output is not None and not schema_registry.has(node.schema_refs.output):
            raise PipelineDefinitionError(
                f"Node '{node.name}' references unknown output schema '{node.schema_refs.output}'"
            )
