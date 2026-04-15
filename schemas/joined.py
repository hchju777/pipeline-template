from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import Series


class JoinedACSchema(pa.DataFrameModel):
    id: Series[int]
    name: Series[str]
    c_score: Series[int]
    c_grade: Series[str]
