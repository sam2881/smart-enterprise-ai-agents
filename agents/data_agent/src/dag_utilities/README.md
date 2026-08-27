# DAG Utilities

42 building-block modules used by the APEX GeneratorAgent when constructing Airflow DAGs. These are **generated into** the output DAG — not called directly at runtime.

The GeneratorAgent (`../agents/generator_agent.py`) imports these to assemble pipeline DAGs before writing them via Jinja2 templates.

## Sub-packages

| Package | Files | Purpose |
|---------|-------|---------|
| `zone_processors/` | 4 | Medallion zone data processing (Landing → Bronze → Silver → Gold) |
| `spark_wrappers/` | 4 | PySpark session management, configuration, cluster sizing |
| `sql_executors/` | 4 | BigQuery + PostgreSQL query execution helpers |
| `zone_transitions/` | 3 | Zone promotion logic, lineage recording |
| `data_quality/` | 5 | Great Expectations suite builders, assertion generators |
| `schema_handlers/` | 4 | Schema detection, evolution, Bronze enforcement |
| `pii_handlers/` | 3 | PII column detection + zone-appropriate masking |
| `partitioning/` | 3 | Date/composite/bucketed partition strategy selection |
| `monitoring/` | 4 | DAG SLA callbacks, failure handlers, alerting hooks |
| `connectors/` | 5 | Source-type-specific Airflow connection factories |
| `utilities/` | 7 | Logging, retry decorators, date math, config loaders |

## Key Files

| File | Purpose |
|------|---------|
| `zone_processor.py` | Base class for all zone processors |
| `spark_wrapper.py` | PySpark session factory with auto-config |
| `sql_executor.py` | Async BigQuery + PostgreSQL executor |
| `zone_transition.py` | Submission and promotion between medallion zones |

## Usage Pattern

```python
# GeneratorAgent uses these as imports in generated DAG code
from dag_utilities.common.zone_transition import get_zone_table_path
from dag_utilities.common.spark_wrapper import get_spark_session
```

Generated DAGs land in `dags/` — do not hand-edit files in `dags/`, they are overwritten on re-deploy.
