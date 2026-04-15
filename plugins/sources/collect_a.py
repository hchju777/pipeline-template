from __future__ import annotations

import pandas as pd

from kernel.interfaces import NodePlugin


class CollectAPlugin(NodePlugin):
    name = "collect_a"
    category = "source"

    def process(self, inputs: dict, context, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"id": 1, "name": " Alice "},
                {"id": 2, "name": "BOB "},
                {"id": 3, "name": " Charlie"},
                {"id": 4, "name": "Eve"},
            ]
        )
