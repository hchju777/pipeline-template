from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd


class CustomTTLCache:
    def __init__(
        self,
        max_entries: int = 128,
        copy_on_read: bool = True,
        copy_on_write: bool = True,
    ) -> None:
        self.max_entries = max_entries
        self.copy_on_read = copy_on_read
        self.copy_on_write = copy_on_write
        self._store: dict[str, tuple[Any, datetime]] = {}

    def _copy_value(self, value: Any) -> Any:
        if isinstance(value, pd.DataFrame):
            return value.copy(deep=True)
        return deepcopy(value)

    def cleanup(self) -> None:
        now = datetime.now(UTC)
        expired_keys = [key for key, (_, expires_at) in self._store.items() if expires_at <= now]
        for key in expired_keys:
            self._store.pop(key, None)

    def has(self, key: str) -> bool:
        self.cleanup()
        return key in self._store

    def get(self, key: str) -> Any:
        self.cleanup()
        value, _ = self._store[key]
        return self._copy_value(value) if self.copy_on_read else value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        self.cleanup()
        stored_value = self._copy_value(value) if self.copy_on_write else value
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        self._store[key] = (stored_value, expires_at)

        if len(self._store) > self.max_entries:
            oldest_key = min(self._store.items(), key=lambda item: item[1][1])[0]
            self._store.pop(oldest_key, None)

    def clear(self) -> None:
        self._store.clear()
