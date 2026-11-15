-- A partially published day must never be reported as an anomaly: that was the
-- false "critical" on the first real run (NYC's latest day read as a citywide collapse).
select anomaly_key
from {{ ref('mart_volume_anomalies') }}
where is_provisional and is_anomaly
