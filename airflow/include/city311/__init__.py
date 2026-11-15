"""Reusable, Airflow-free building blocks for the city311 pipeline.

Keeping this code out of the DAG files means it can be unit-tested with plain
pytest (no scheduler, no providers) and reused by scripts and CI.
"""
