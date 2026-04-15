from __future__ import annotations

import pandas as pd

from kernel.interfaces import NodePlugin


class CollectCPlugin(NodePlugin):
    name = "collect_c"
    category = "source"

    def process(self, inputs: dict, context, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"id": 1, "score": 90, "grade": "A"},
                {"id": 2, "score": 70, "grade": "B"},
                {"id": 3, "score": 85, "grade": "A"},
            ]
        )
