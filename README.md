# Pipeline Engine Template

이 프로젝트는 DataFrame 기반 ETL/ELT 작업을 작은 플러그인으로 나누고, JSON에 정의된 DAG 순서대로 실행하는 파이프라인 엔진 템플릿입니다.

핵심 아이디어는 단순합니다. 파이프라인의 구조와 옵션은 JSON/Pydantic으로 관리하고, 실제 데이터 검증은 Pandera 스키마로 관리하며, 각 처리 단계는 독립적인 `NodePlugin` 클래스로 구현합니다.

## 주요 기능

- DAG 기반 실행: `depends_on` 관계를 기준으로 노드를 위상 정렬한 뒤 순서대로 실행합니다.
- 플러그인 구조: source, transform, filter, join, sink 같은 작업을 독립 클래스 단위로 추가할 수 있습니다.
- 자동 플러그인 등록: `plugins/` 패키지 아래의 `NodePlugin` 하위 클래스를 자동 탐색해 등록합니다.
- Python 또는 JSON 기반 파이프라인 정의: 노드 이름, 플러그인, 입력 연결, 옵션, 스키마, 캐시, 태그를 파일에서 선언합니다.
- Python 파이프라인 지원: 반복 설정은 Python helper 함수로 줄이고, 최종 결과는 같은 `PipelineDefinition` 모델로 검증합니다.
- Pydantic 옵션 검증: 각 플러그인의 옵션 모델이 실행 전에 옵션 형태를 검증합니다.
- Pandera DataFrame 검증: 입력/출력 DataFrame이 등록된 스키마와 맞는지 검증합니다.
- Validation mode: `strict`, `boundary_only`, `off` 모드로 검증 강도를 조절합니다.
- 노드별 검증 override: 개별 노드가 전역 검증 모드를 상속하거나 `strict`, `boundary_only`, `off`로 덮어쓸 수 있습니다.
- TTL 캐시: 노드 입력 fingerprint와 옵션을 기반으로 캐시 키를 만들고 결과를 재사용합니다.
- Subset 실행: 특정 tag 또는 node만 실행하되 필요한 upstream 의존성은 자동 포함합니다.
- Downstream 확장: 선택한 노드 이후의 downstream 노드까지 함께 실행할 수 있습니다.
- 실행 기록: 노드별 duration, input/output row count, cache key prefix, effective validation mode를 기록합니다.
- 테스트 포함: 전체 실행, subset 실행, 캐시, 스키마 등록, 파이프라인 검증 테스트가 포함되어 있습니다.

## 빠른 시작

Windows PowerShell 기준 예시입니다.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe app.py
```

가상환경을 활성화했다면 아래처럼 실행해도 됩니다.

```powershell
.\.venv\Scripts\Activate.ps1
python app.py
```

## 테스트 실행

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

현재 테스트는 `pytest.ini` 설정에 따라 `tests/` 디렉터리만 수집합니다.

## 프로젝트 구조

```text
pipeline-template/
  app.py
  requirements.txt
  pytest.ini
  common/
    hashing.py
  context/
    pipeline_engine_spec_ai.md
  kernel/
    cache.py
    context.py
    dag_kernel.py
    exceptions.py
    interfaces.py
    loader.py
    models.py
    plugin_auto_loader.py
    registry.py
    schema_registry.py
    validation.py
  pipelines/
    customer_pipeline.py
    customer_pipeline.json
  plugins/
    validation.py
    sources/
    transforms/
    filters/
    joins/
    sinks/
  schemas/
    allowed.py
    customer.py
    joined.py
    score.py
  tests/
```

각 디렉터리의 역할은 다음과 같습니다.

| 경로 | 역할 |
| --- | --- |
| `app.py` | CLI 엔트리포인트입니다. 플러그인/스키마 등록, 파이프라인 로딩, 커널 실행을 조립합니다. |
| `common/` | 여러 계층에서 공유하는 유틸리티를 둡니다. 현재는 캐시 키 생성을 위한 hashing 유틸이 있습니다. |
| `kernel/` | 파이프라인 엔진의 핵심입니다. DAG 실행, 모델, 레지스트리, 검증, 캐시, 예외가 들어 있습니다. |
| `pipelines/` | JSON 파이프라인 정의를 둡니다. 어떤 노드를 어떤 순서로 실행할지 선언합니다. |
| `plugins/` | 실제 데이터 처리 단계를 구현합니다. 파일을 추가하면 자동 등록 대상이 됩니다. |
| `schemas/` | Pandera DataFrame schema를 둡니다. 플러그인과 분리되어 있어 재사용하기 쉽습니다. |
| `tests/` | 엔진 동작과 샘플 파이프라인을 검증하는 pytest 테스트입니다. |

## 실행 흐름

전체 실행 흐름은 아래 순서로 진행됩니다.

```text
JSON pipeline
  -> Pydantic model load
  -> plugin auto-registration
  -> schema registration
  -> pipeline definition validation
  -> subset selection
  -> topological sort
  -> input resolution
  -> option validation
  -> input schema validation
  -> cache lookup
  -> plugin process
  -> output schema validation
  -> cache save
  -> execution record append
```

중요한 점은 `inputs`와 `depends_on`이 함께 맞아야 한다는 것입니다. 예를 들어 어떤 노드가 `"inputs": {"source": "collect_a"}`를 사용한다면 `"depends_on": ["collect_a"]`에도 같은 upstream 노드가 들어 있어야 합니다. JSON 파일은 이 관계를 명시적으로 적고, Python 파일은 helper 함수가 `inputs`에서 `depends_on`을 만들어 줍니다. 최종 검증은 `kernel.loader.validate_pipeline_definition()`에서 실행 전에 수행됩니다.

## 샘플 파이프라인

샘플 파이프라인은 `pipelines/customer_pipeline.py`에 있습니다. 같은 내용을 명시적인 JSON으로 적은 `pipelines/customer_pipeline.json`도 함께 제공합니다.

```text
collect_a ──> a_trim_name ──> a_lower_name ──┐
                                             ├─> merge_ac ──> filter_with_b ──> output
collect_c ──> c_joinable ───────────────────┘                  ▲
                                                                │
collect_b ──────────────────────────────────────────────────────┘
```

각 노드의 의미는 다음과 같습니다.

| 노드 | 플러그인 | 설명 |
| --- | --- | --- |
| `collect_a` | `collect_a` | 고객 기본 데이터인 `id`, `name`을 생성합니다. |
| `collect_b` | `collect_b` | 허용할 고객 ID 목록인 `allowed_id`를 생성합니다. |
| `collect_c` | `collect_c` | 고객별 점수 데이터인 `id`, `score`, `grade`를 생성합니다. |
| `a_trim_name` | `trim_field` | `name` 컬럼의 앞뒤 공백을 제거합니다. |
| `a_lower_name` | `lowercase_field` | `name` 컬럼을 소문자로 변환합니다. |
| `c_joinable` | `filter_required_fields` | join에 필요한 `id`, `score`, `grade`가 비어 있지 않은 row만 남깁니다. |
| `merge_ac` | `join_on_key` | 고객 데이터와 점수 데이터를 `id` 기준으로 join합니다. |
| `filter_with_b` | `filter_by_reference` | `collect_b`에 포함된 ID만 최종 결과에 남깁니다. |
| `output` | `print_result` | 최종 DataFrame을 출력하고 그대로 반환합니다. |

기본 실행 결과는 `id=1`, `id=3` 두 row입니다.

```text
   id     name  c_score c_grade
0   1    alice       90       A
1   3  charlie       85       A
```

## CLI 사용법

전체 파이프라인을 실행합니다.

```powershell
python app.py
```

다른 Python 또는 JSON 파이프라인 파일을 지정합니다.

```powershell
python app.py --pipeline pipelines/customer_pipeline.py
python app.py --pipeline pipelines/customer_pipeline.json
```

특정 tag를 가진 노드와 그 upstream 의존성만 실행합니다.

```powershell
python app.py --tags report
```

특정 tag를 가진 노드, 그 upstream 의존성, 그리고 downstream 노드까지 실행합니다.

```powershell
python app.py --tags join --include-downstream
```

특정 node와 그 upstream 의존성만 실행합니다.

```powershell
python app.py --nodes merge_ac
```

검증 모드를 지정합니다.

```powershell
python app.py --validation-mode strict
python app.py --validation-mode boundary_only
python app.py --validation-mode off
```

## 파이프라인 정의 구조

기본 권장 방식은 Python 파일입니다. Python 파일은 `get_pipeline()` 함수 또는 `pipeline` 변수를 노출해야 합니다.

```python
from kernel.models import NodeDefinition, PipelineDefinition, SchemaRefs


def get_pipeline() -> PipelineDefinition:
    return PipelineDefinition(
        version="1.0",
        pipeline_name="customer_pipeline",
        nodes=[
            NodeDefinition(
                name="collect_a",
                plugin="collect_a",
                schema_refs=SchemaRefs(output="customer_base"),
                tags=["source"],
                cache=True,
                cache_ttl_seconds=300,
            )
        ],
    )
```

`pipelines/customer_pipeline.py`는 `node()` helper를 사용합니다. 그래서 `inputs`를 한 번만 적으면 helper가 `depends_on`을 자동으로 채우고, 반복되는 캐시 기본값도 Python 상수로 관리합니다. 이 방식은 JSON 포맷을 바꾸지 않으면서도 작성량을 줄이는 장점이 있습니다.

파이프라인은 `PipelineDefinition` 모델로 로드됩니다.

```json
{
  "version": "1.0",
  "pipeline_name": "customer_pipeline",
  "nodes": []
}
```

각 노드는 `NodeDefinition` 모델로 로드됩니다.

```json
{
  "name": "a_trim_name",
  "plugin": "trim_field",
  "enabled": true,
  "depends_on": ["collect_a"],
  "inputs": {
    "source": "collect_a"
  },
  "options": {
    "source_key": "source",
    "field": "name"
  },
  "schema_refs": {
    "inputs": {
      "source": "customer_base"
    },
    "output": "customer_base"
  },
  "tags": ["normalize"],
  "cache": true,
  "cache_ttl_seconds": 300,
  "validation_mode": "inherit"
}
```

노드 필드의 의미는 다음과 같습니다.

| 필드 | 설명 |
| --- | --- |
| `name` | 파이프라인 안에서 유일해야 하는 노드 이름입니다. |
| `plugin` | 사용할 플러그인 이름입니다. 플러그인 클래스의 `name` 값과 일치해야 합니다. |
| `enabled` | `false`면 실행 대상에서 제외됩니다. |
| `depends_on` | 이 노드보다 먼저 실행되어야 하는 upstream 노드 목록입니다. |
| `inputs` | 플러그인에 넘길 입력 alias와 upstream 노드 이름의 매핑입니다. |
| `options` | 플러그인별 Pydantic option model로 검증되는 설정입니다. |
| `schema_refs.inputs` | 입력 alias별 Pandera schema 이름입니다. |
| `schema_refs.output` | 출력 DataFrame에 적용할 Pandera schema 이름입니다. |
| `tags` | subset 실행에서 사용할 태그입니다. |
| `cache` | `true`면 노드 결과를 TTL 캐시에 저장합니다. |
| `cache_ttl_seconds` | 캐시 유지 시간입니다. 생략하면 실행 시 기본값 `300`초를 사용합니다. |
| `on_error` | `"raise"` 또는 `"skip"`입니다. 기본값은 `"raise"`입니다. |
| `validation_mode` | `"inherit"`, `"strict"`, `"boundary_only"`, `"off"` 중 하나입니다. 기본값은 `"inherit"`이며 전역 CLI 검증 모드를 따릅니다. |

## 플러그인 구조

모든 플러그인은 `kernel.interfaces.NodePlugin`을 상속합니다.

```python
class NodePlugin(ABC):
    name: str = "base"
    version: str = "1.0.0"
    category: str = "generic"

    option_model: type[BaseModel] = EmptyOptions
    required_input_aliases: set[str] = set()

    def validate_inputs(self, inputs: dict[str, Any]) -> None: ...
    def validate_options(self, options: dict[str, Any]) -> BaseModel: ...
    def process(self, inputs: dict[str, Any], context, **kwargs) -> Any: ...
```

플러그인을 추가할 때 지켜야 할 규칙은 다음과 같습니다.

- `name`은 전체 플러그인 중 유일해야 합니다.
- `category`는 검증 모드에서 사용됩니다. 일반적으로 `source`, `normalize`, `filter`, `join`, `sink` 중 하나를 사용합니다.
- `option_model`에는 해당 플러그인이 받을 옵션 구조를 Pydantic 모델로 정의합니다.
- `required_input_aliases`에는 JSON `inputs`에 반드시 있어야 하는 alias를 적습니다.
- `process()`는 입력을 직접 수정하지 말고 새 객체를 반환해야 합니다.
- DataFrame 입력은 `plugins.validation.get_dataframe_input()`을 사용하면 alias/type 검증과 deep copy를 함께 처리할 수 있습니다.
- 필요한 컬럼은 `plugins.validation.require_columns()`로 명시적으로 확인하는 것이 좋습니다.

간단한 transform 플러그인 예시는 다음과 같습니다.

```python
from __future__ import annotations

import pandas as pd
from pydantic import BaseModel, field_validator

from kernel.interfaces import NodePlugin
from plugins.validation import get_dataframe_input, require_columns


class TrimFieldOptions(BaseModel):
    source_key: str
    field: str

    @field_validator("source_key", "field")
    @classmethod
    def non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be non-empty")
        return value


class TrimFieldPlugin(NodePlugin):
    name = "trim_field"
    category = "normalize"
    option_model = TrimFieldOptions
    required_input_aliases = {"source"}

    def process(self, inputs: dict, context, source_key: str, field: str) -> pd.DataFrame:
        df = get_dataframe_input(inputs, source_key, self.name)
        require_columns(df, [field], self.name)
        df[field] = df[field].apply(lambda x: x.strip() if isinstance(x, str) else x)
        return df
```

## 내장 플러그인

현재 포함된 플러그인은 다음과 같습니다.

| 플러그인 | 카테고리 | 위치 | 설명 |
| --- | --- | --- | --- |
| `collect_a` | `source` | `plugins/sources/collect_a.py` | 고객 기본 데이터를 생성합니다. |
| `collect_b` | `source` | `plugins/sources/collect_b.py` | 허용 ID 목록을 생성합니다. |
| `collect_c` | `source` | `plugins/sources/collect_c.py` | 점수 데이터를 생성합니다. |
| `trim_field` | `normalize` | `plugins/transforms/trim_field.py` | 문자열 컬럼의 앞뒤 공백을 제거합니다. |
| `lowercase_field` | `normalize` | `plugins/transforms/lowercase_field.py` | 문자열 컬럼을 소문자로 변환합니다. |
| `filter_required_fields` | `filter` | `plugins/filters/filter_required_fields.py` | 필수 컬럼이 비어 있지 않은 row만 남깁니다. |
| `filter_by_reference` | `filter` | `plugins/filters/filter_by_reference.py` | reference DataFrame의 key 목록에 포함된 row만 남깁니다. |
| `join_on_key` | `join` | `plugins/joins/join_on_key.py` | 두 DataFrame을 key 기준으로 join합니다. |
| `print_result` | `sink` | `plugins/sinks/print_result.py` | DataFrame을 출력하고 그대로 반환합니다. |

## 스키마 구조

스키마는 Pandera `DataFrameModel`로 정의합니다.

```python
from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import Series


class CustomerSchema(pa.DataFrameModel):
    id: Series[int]
    name: Series[str]
```

정의한 스키마는 `schemas/__init__.py`의 `register_all_schemas()`에서 이름과 함께 등록합니다.

```python
def register_all_schemas(registry: SchemaRegistry) -> None:
    registry.register("customer_base", CustomerSchema)
```

파이프라인 JSON에서는 등록된 이름을 참조합니다.

```json
{
  "schema_refs": {
    "inputs": {
      "source": "customer_base"
    },
    "output": "customer_base"
  }
}
```

## 검증 모드

검증 모드는 `kernel.validation.ValidationMode`에 정의되어 있습니다.

| 모드 | 설명 |
| --- | --- |
| `strict` | 모든 플러그인의 입력/출력 DataFrame을 스키마로 검증합니다. 가장 안전하지만 가장 느릴 수 있습니다. |
| `boundary_only` | `source`, `join`, `sink` 카테고리만 검증합니다. 기본값이며 비용과 안정성의 균형을 잡습니다. |
| `off` | Pandera DataFrame 검증을 끕니다. 옵션 검증과 플러그인 자체 검증은 여전히 동작합니다. |

주의할 점은 `boundary_only`에서 `normalize`, `filter` 카테고리의 중간 결과는 Pandera 검증을 건너뛴다는 것입니다. 대신 현재 플러그인들은 alias, DataFrame type, 필수 컬럼을 자체적으로 확인하도록 구현되어 있습니다.

개별 노드가 전역 모드와 다른 검증 강도를 가져야 한다면 JSON 노드에 `validation_mode`를 지정할 수 있습니다.

```json
{
  "name": "important_filter",
  "plugin": "filter_required_fields",
  "validation_mode": "strict"
}
```

`"inherit"`은 전역 `--validation-mode` 값을 그대로 사용합니다. `"off"`를 지정하면 해당 노드의 Pandera 입력/출력 검증만 끄고, Pydantic 옵션 검증과 플러그인 내부 검증은 유지합니다.

## Subset 실행 규칙

`--tags` 또는 `--nodes`를 사용하면 전체 DAG 중 일부만 실행할 수 있습니다.

기본 규칙은 다음과 같습니다.

- 선택된 노드는 target node가 됩니다.
- target node가 실행되기 위해 필요한 모든 upstream 의존성은 자동 포함됩니다.
- downstream 노드는 기본적으로 포함되지 않습니다.
- `--include-downstream`을 추가하면 target 이후 downstream 노드도 포함됩니다.
- downstream 노드가 별도의 upstream 의존성을 필요로 하면 그 upstream도 자동 포함됩니다.
- 실행 대상 노드 목록은 원본 JSON 노드 순서를 기준으로 결정되므로 실행 기록과 캐시 동작을 재현하기 쉽습니다.

예를 들어 아래 명령은 `join` 태그가 붙은 `merge_ac`와 그 upstream만 실행합니다.

```powershell
python app.py --tags join
```

아래 명령은 `merge_ac` 이후의 `filter_with_b`, `output`도 실행합니다. 이때 `filter_with_b`가 필요로 하는 `collect_b`도 자동 포함됩니다.

```powershell
python app.py --tags join --include-downstream
```

## 에러 처리와 Skip 전파

노드의 `on_error` 기본값은 `"raise"`입니다. 이 경우 플러그인 실행, 옵션 검증, 스키마 검증 중 예외가 발생하면 파이프라인 전체가 즉시 중단됩니다.

`on_error`를 `"skip"`으로 지정하면 해당 노드는 `skipped` 상태로 기록되고 파이프라인은 계속 진행합니다. 단, skipped 노드에 의존하는 downstream 노드는 사용할 upstream 결과가 없으므로 함께 `skipped` 처리됩니다. 이 전파 정책 덕분에 downstream에서 별도의 `upstream result unavailable` 오류가 늦게 터지는 대신, 실행 기록에서 어떤 upstream 때문에 skip됐는지 바로 확인할 수 있습니다.

실행 기록에는 다음과 같은 관측 정보가 남습니다.

- `duration_ms`: 노드 실행에 걸린 시간입니다.
- `input_row_counts`: 입력 alias별 row count입니다.
- `output_row_count`와 `row_count`: 출력 row count입니다.
- `cache_key_prefix`: 캐시가 켜진 노드의 캐시 키 앞 12자리입니다.
- `validation_mode`: 해당 노드에 실제 적용된 effective validation mode입니다.

## 캐시 동작

캐시는 `kernel.cache.CustomTTLCache`가 담당합니다.

캐시 키는 다음 정보를 기반으로 생성됩니다.

- 파이프라인 이름과 버전
- 노드 이름
- 플러그인 이름
- 플러그인 버전
- 검증된 옵션 값
- 입력 DataFrame 또는 입력 값의 fingerprint
- 입력/출력 schema reference
- 해당 노드에 적용된 validation mode

DataFrame fingerprint는 컬럼 순서를 안정화한 뒤 CSV 표현을 hash해서 만듭니다. 캐시는 DataFrame을 저장하거나 읽을 때 deep copy를 수행하므로, 캐시에서 꺼낸 값을 수정해도 원본 캐시 값이 오염되지 않습니다.

캐시 설정 예시는 다음과 같습니다.

```json
{
  "cache": true,
  "cache_ttl_seconds": 300
}
```

## 새 플러그인 추가 절차

새 플러그인을 추가할 때는 보통 아래 순서로 진행합니다.

1. `plugins/` 아래 적절한 하위 디렉터리에 새 `.py` 파일을 만듭니다.
2. Pydantic option model을 정의합니다.
3. `NodePlugin`을 상속한 클래스를 만들고 `name`, `category`, `option_model`, `required_input_aliases`를 지정합니다.
4. `process()`에서 입력 DataFrame을 복사한 뒤 새 결과를 반환합니다.
5. 필요한 경우 `schemas/`에 새 Pandera schema를 추가합니다.
6. `schemas/__init__.py`에 schema 등록 코드를 추가합니다.
7. `pipelines/*.py` 또는 `pipelines/*.json`에 새 node를 추가합니다.
8. `tests/`에 최소한 happy path와 실패 케이스 테스트를 추가합니다.

플러그인 자동 등록은 `plugins/` 패키지를 순회하면서 `NodePlugin` 하위 클래스를 찾는 방식입니다. 따라서 일반적인 경우 별도 registry 코드를 수정할 필요가 없습니다.

## 새 파이프라인 추가 절차

새 파이프라인을 만들 때는 아래 내용을 확인하세요.

1. `pipeline_name`과 `version`을 지정합니다.
2. 각 노드의 `name`이 중복되지 않게 합니다.
3. `plugin` 값이 실제 등록된 플러그인 이름과 일치하는지 확인합니다.
4. `depends_on`에는 실행 순서상 필요한 upstream 노드를 모두 적습니다.
5. `inputs`에는 플러그인이 받을 alias와 upstream 노드 이름을 매핑합니다.
6. JSON에서는 `inputs`에 적은 upstream 노드를 반드시 `depends_on`에도 포함합니다. Python helper를 쓴다면 helper가 대신 채울 수 있습니다.
7. `options`는 플러그인의 option model과 맞춰 작성합니다.
8. `schema_refs`는 `schemas/__init__.py`에 등록된 schema 이름만 사용합니다.
9. 캐시가 필요한 노드는 `cache: true`와 적절한 TTL을 지정합니다.
10. 반복이 많다면 JSON 대신 Python 파일과 helper 함수로 줄입니다.

## 테스트 구성

현재 테스트는 다음 영역을 확인합니다.

| 테스트 파일 | 확인 내용 |
| --- | --- |
| `tests/test_full_run.py` | 전체 파이프라인이 끝까지 실행되고 최종 결과 컬럼/row 수가 맞는지 확인합니다. |
| `tests/test_subset_execution.py` | tag 기반 subset 실행, upstream 자동 포함, downstream 확장을 확인합니다. |
| `tests/test_execution_policies.py` | skip 전파, cache key의 plugin version 반영, execution record 관측 필드를 확인합니다. |
| `tests/test_validation_modes.py` | 전역 validation mode와 노드별 validation override 동작을 확인합니다. |
| `tests/test_pipeline_loading.py` | JSON/Python 파일이 `PipelineDefinition`으로 로드되는지 확인합니다. |
| `tests/test_pipeline_validation.py` | 잘못된 input alias, dependency 누락, option 오류를 정의 검증 단계에서 잡는지 확인합니다. |
| `tests/test_custom_cache.py` | TTL 캐시의 저장/조회/copy/만료 동작을 확인합니다. |
| `tests/test_schema_registry.py` | schema registry 등록/조회 동작을 확인합니다. |

## 개발 시 주의사항

- 플러그인은 stateless하게 작성하는 것이 좋습니다.
- 입력 DataFrame을 직접 수정하지 말고 반드시 복사한 뒤 결과를 반환하세요.
- JSON의 `inputs`와 플러그인의 `required_input_aliases`가 맞아야 합니다.
- JSON의 `inputs`에서 참조하는 upstream 노드는 `depends_on`에도 있어야 합니다.
- `boundary_only` 모드에서는 일부 중간 노드의 Pandera 검증이 생략될 수 있으므로, 플러그인 내부의 컬럼 검증을 적극적으로 넣는 것이 좋습니다.
- 캐시 키는 입력 fingerprint, 옵션, 파이프라인 버전, 플러그인 버전, 스키마 참조, validation mode에 기반하므로, 처리 의미가 바뀌면 기존 캐시와 분리됩니다.
- 운영용으로 확장한다면 logging backend, retry policy, parallel execution, persistent cache 같은 기능을 추가할 수 있습니다.

## 참고 스펙

`context/pipeline_engine_spec_ai.md`에는 이 템플릿을 만들 때 기준으로 삼은 AI 최적화 설계 스펙이 들어 있습니다. 엔진을 크게 확장하거나 구조를 바꿀 때 먼저 읽어보면 방향을 잡기 좋습니다.
