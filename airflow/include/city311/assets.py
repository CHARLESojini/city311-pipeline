"""Assets shared between DAGs (data-aware scheduling)."""
from airflow.sdk import Asset

# Updated by city311_ingest once every source is loaded; triggers city311_transform.
RAW_ASSET = Asset(name="city311_raw")

# Updated by city311_transform after dbt builds the gold marts.
GOLD_ASSET = Asset(name="city311_gold")
