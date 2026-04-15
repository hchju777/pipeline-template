from kernel.schema_registry import SchemaRegistry

from schemas.allowed import AllowedIdSchema
from schemas.customer import CustomerSchema
from schemas.joined import JoinedACSchema
from schemas.score import ScoreSchema


def register_all_schemas(registry: SchemaRegistry) -> None:
    registry.register("customer_base", CustomerSchema)
    registry.register("allowed_id_base", AllowedIdSchema)
    registry.register("score_base", ScoreSchema)
    registry.register("joined_ac", JoinedACSchema)
