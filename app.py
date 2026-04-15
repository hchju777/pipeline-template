from __future__ import annotations

import argparse

from kernel.cache import CustomTTLCache
from kernel.context import PipelineContext
from kernel.dag_kernel import DAGPipelineKernel
from kernel.loader import load_pipeline, validate_pipeline_definition
from kernel.plugin_auto_loader import auto_register_plugins
from kernel.registry import PluginRegistry
from kernel.schema_registry import SchemaRegistry
from kernel.validation import ValidationMode
from schemas import register_all_schemas


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pipeline",
        default="pipelines/customer_pipeline.py",
        help="Path to pipeline definition (.py or .json)",
    )
    parser.add_argument(
        "--tags",
        nargs="*",
        default=None,
        help="Run nodes selected by tags, including all upstream dependencies",
    )
    parser.add_argument(
        "--nodes",
        nargs="*",
        default=None,
        help="Run specified node names, including all upstream dependencies",
    )
    parser.add_argument(
        "--include-downstream",
        action="store_true",
        help="Also include downstream nodes from selected subset roots",
    )
    parser.add_argument(
        "--validation-mode",
        choices=[mode.value for mode in ValidationMode],
        default=ValidationMode.BOUNDARY_ONLY.value,
        help="Validation mode",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    plugin_registry = PluginRegistry()
    auto_register_plugins(plugin_registry, root_package="plugins")

    schema_registry = SchemaRegistry()
    register_all_schemas(schema_registry)

    pipeline = load_pipeline(args.pipeline)
    validate_pipeline_definition(pipeline, plugin_registry, schema_registry)

    context = PipelineContext(
        metadata={
            "pipeline_name": pipeline.pipeline_name,
            "version": pipeline.version,
        }
    )

    cache = CustomTTLCache(max_entries=128, copy_on_read=True, copy_on_write=True)
    kernel = DAGPipelineKernel(
        registry=plugin_registry,
        schema_registry=schema_registry,
        cache=cache,
        validation_mode=ValidationMode(args.validation_mode),
    )

    results = kernel.run(
        pipeline=pipeline,
        context=context,
        tags=args.tags,
        node_names=args.nodes,
        include_downstream=args.include_downstream,
    )

    print("\n=== REGISTERED PLUGINS ===")
    print(plugin_registry.list_plugins())

    print("\n=== REGISTERED SCHEMAS ===")
    print(schema_registry.list_names())

    print("\n=== RESULT KEYS ===")
    print(list(results.keys()))

    print("\n=== EXECUTION RECORDS ===")
    for record in context.execution_records:
        print(record.model_dump())


if __name__ == "__main__":
    main()
