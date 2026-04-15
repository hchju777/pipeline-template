from __future__ import annotations

import pandas as pd

from kernel.interfaces import NodePlugin


class CollectBPlugin(NodePlugin):
    name = "collect_b"
    category = "source"

    def process(self, inputs: dict, context, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"allowed_id": 1},
                {"allowed_id": 3},
            ]
        )
