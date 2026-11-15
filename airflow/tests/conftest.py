import sys
from pathlib import Path

# Make `include.*` importable the same way Airflow does inside the Astro image
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
