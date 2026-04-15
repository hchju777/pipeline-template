from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import Series


class ScoreSchema(pa.DataFrameModel):
    id: Series[int]
    score: Series[int]
    grade: Series[str]
