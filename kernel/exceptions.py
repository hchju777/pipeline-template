from __future__ import annotations


class PipelineError(Exception):
    pass


class PipelineDefinitionError(PipelineError):
    pass


class CycleDetectedError(PipelineDefinitionError):
    pass


class PluginRegistrationError(PipelineError):
    pass


class PluginNotFoundError(PipelineError):
    pass


class PluginExecutionError(PipelineError):
    pass


class InputValidationError(PipelineError):
    pass


class SchemaRegistryError(PipelineError):
    pass
