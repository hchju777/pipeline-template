from kernel.schema_registry import SchemaRegistry
from schemas.customer import CustomerSchema


def test_schema_registry_register_and_get():
    registry = SchemaRegistry()
    registry.register("customer_base", CustomerSchema)
    assert registry.has("customer_base")
    assert registry.get("customer_base") is CustomerSchema
