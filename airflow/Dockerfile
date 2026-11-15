FROM astrocrpublic.azurecr.io/runtime:3.3-8

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's.
# The dbt project itself is mounted at /usr/local/airflow/dbt (docker-compose.override.yml).
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core~=1.12.0" "dbt-snowflake~=1.12.0"
