from __future__ import annotations

from typing import Type

from kernel.exceptions import PluginNotFoundError, PluginRegistrationError
from kernel.interfaces import NodePlugin


class PluginRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, Type[NodePlugin]] = {}

    def register(self, plugin_cls: Type[NodePlugin]) -> None:
        if not issubclass(plugin_cls, NodePlugin):
            raise PluginRegistrationError(
                f"{plugin_cls.__name__} is not a NodePlugin subclass"
            )

        plugin_name = plugin_cls.name
        if not plugin_name or plugin_name == "base":
            raise PluginRegistrationError(
                f"{plugin_cls.__name__} has invalid plugin name '{plugin_name}'"
            )
        if plugin_name in self._plugins:
            raise PluginRegistrationError(
                f"Plugin '{plugin_name}' is already registered"
            )

        self._plugins[plugin_name] = plugin_cls

    def create(self, plugin_name: str) -> NodePlugin:
        plugin_cls = self._plugins.get(plugin_name)
        if plugin_cls is None:
            available = ", ".join(sorted(self._plugins.keys()))
            raise PluginNotFoundError(
                f"Plugin '{plugin_name}' not found. Available: [{available}]"
            )
        return plugin_cls()

    def has(self, plugin_name: str) -> bool:
        return plugin_name in self._plugins

    def list_plugins(self) -> list[str]:
        return sorted(self._plugins.keys())
