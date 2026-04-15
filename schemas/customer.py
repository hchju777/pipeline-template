from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import Series


class CustomerSchema(pa.DataFrameModel):
    id: Series[int]
    name: Series[str]
