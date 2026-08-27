"""
Standalone PySpark Scripts for Dataproc Execution

These scripts are designed to run on Dataproc via spark-submit.
They CANNOT import from dag_utilities at runtime (not available on worker nodes
unless bundled). Instead, they receive ALL parameters via sys.argv positional
arguments and connect to PostgreSQL directly via psycopg2.

Scripts:
    - ge_schema_validator.py: Schema + semantic GE validation
    - consumption_zone.py: Consumption zone with Data Vault patterns
"""
