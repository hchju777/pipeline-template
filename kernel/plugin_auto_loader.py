from __future__ import annotations

import importlib
import pkgutil
from types import ModuleType

from kernel.interfaces import NodePlugin
from kernel.registry import PluginRegistry


def import_submodules(package_name: str) -> list[ModuleType]:
    modules: list[ModuleType] = []
    package = importlib.import_module(package_name)
    if not hasattr(package, "__path__"):
        return modules

    for _, module_name, _ in pkgutil.walk_packages(
        package.__path__, package.__name__ + "."
    ):
        modules.append(importlib.import_module(module_name))
    return modules


def auto_register_plugins(registry: PluginRegistry, root_package: str = "plugins") -> None:
    modules = import_submodules(root_package)

    for module in modules:
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if (
                isinstance(obj, type)
                and issubclass(obj, NodePlugin)
                and obj is not NodePlugin
            ):
                registry.register(obj)
