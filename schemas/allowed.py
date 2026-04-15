from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import Series


class AllowedIdSchema(pa.DataFrameModel):
    allowed_id: Series[int]
