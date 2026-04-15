from __future__ import annotations

from enum import Enum


class ValidationMode(str, Enum):
    STRICT = "strict"
    BOUNDARY_ONLY = "boundary_only"
    OFF = "off"
