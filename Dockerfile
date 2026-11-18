FROM apache/airflow:2.8.1-python3.11

USER airflow

# The Postgres provider is installed in its own step, with a pin, for two
# reasons:
# 1. Unpinned, pip takes the newest release, which is built for Airflow 3 and
#    pulls in fastapi, uvicorn and psycopg 3. Nothing fails until a build
#    without a cache resolves it again.
# 2. In the same pip run as the pinned dbt packages below, the resolver has
#    to reconcile them with the whole Airflow dependency tree and backtracks
#    for hours.
# 5.10.0 is the version in Airflow 2.8.1's own constraints file
# (constraints-2.8.1/constraints-3.11.txt).
RUN pip install --no-cache-dir apache-airflow-providers-postgres==5.10.0

COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt