from kernel.loader import load_pipeline, load_pipeline_from_json, load_pipeline_from_python
from kernel.models import PipelineDefinition


def test_load_pipeline():
    pipeline = load_pipeline_from_json("pipelines/customer_pipeline.json")
    assert isinstance(pipeline, PipelineDefinition)
    assert pipeline.pipeline_name == "customer_pipeline"
    assert len(pipeline.nodes) > 0


def test_load_pipeline_from_python():
    pipeline = load_pipeline_from_python("pipelines/customer_pipeline.py")

    collect_a = next(node for node in pipeline.nodes if node.name == "collect_a")
    a_trim_name = next(node for node in pipeline.nodes if node.name == "a_trim_name")
    output = next(node for node in pipeline.nodes if node.name == "output")

    assert collect_a.cache is True
    assert collect_a.cache_ttl_seconds == 300
    assert a_trim_name.depends_on == ["collect_a"]
    assert output.depends_on == ["filter_with_b"]
    assert output.cache is False


def test_load_pipeline_dispatches_by_extension():
    json_pipeline = load_pipeline("pipelines/customer_pipeline.json")
    python_pipeline = load_pipeline("pipelines/customer_pipeline.py")

    assert json_pipeline.pipeline_name == python_pipeline.pipeline_name
    assert [node.name for node in json_pipeline.nodes] == [
        node.name for node in python_pipeline.nodes
    ]
