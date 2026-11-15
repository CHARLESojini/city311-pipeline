"""City 311 dashboard: NYC + Boston service requests from the gold layer.

    streamlit run dashboard/app.py

Reads Snowflake when .streamlit/secrets.toml has a [snowflake] section
(Streamlit Community Cloud: paste the same into the app's Secrets), otherwise
the local DuckDB demo warehouse built from fixtures.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow"))

import data  # noqa: E402, I001
from include.city311.exports import export_queries  # noqa: E402

st.set_page_config(page_title="City 311 pipeline", page_icon=":bar_chart:", layout="wide")

# ---------------------------------------------------------------- palette
# Validated categorical slots (light / dark steps of the same hues). Color follows
# the entity everywhere: NYC is always slot 1, Boston always slot 2.
DARK = getattr(getattr(st.context, "theme", None), "type", "light") == "dark"
LIGHT_STEPS = {"NYC": "#2a78d6", "Boston": "#eb6834", "boston_legacy": "#1baf7a", "boston_new": "#eda100"}
DARK_STEPS = {"NYC": "#3987e5", "Boston": "#d95926", "boston_legacy": "#199e70", "boston_new": "#c98500"}
COLORS = DARK_STEPS if DARK else LIGHT_STEPS
CITY_SCALE = alt.Scale(domain=["NYC", "Boston"], range=[COLORS["NYC"], COLORS["Boston"]])
SYSTEM_LABELS = {"boston_legacy": "Legacy CRM", "boston_new": "New CRM (BCS-)"}
SYSTEM_SCALE = alt.Scale(
    domain=list(SYSTEM_LABELS.values()), range=[COLORS["boston_legacy"], COLORS["boston_new"]]
)
SEVERITY = {
    "info": ("Info", ":material/check_circle:", st.success),
    "warning": ("Warning", ":material/warning:", st.warning),
    "critical": ("Critical", ":material/error:", st.error),
}

be = data.backend()
G, AI = be.gold, be.ai

# ---------------------------------------------------------------- load
try:
    kpis = data.query(f"select * from {G}.mart_city_kpis order by city")
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not read the gold layer ({be.name}). {exc}")
    st.stop()

as_of = pd.to_datetime(kpis["as_of_date"].max()).date()
volume = data.query(f"select * from {G}.mart_daily_city_volume")
volume["activity_date"] = pd.to_datetime(volume["activity_date"])
perf = data.query(f"select * from {G}.mart_category_performance")
backlog = data.query(f"select * from {G}.mart_open_backlog")
anomalies = data.query(f"""
    select activity_date, series_type, city, series_value, direction, request_count,
           baseline_mean, z_score, pct_change_vs_baseline
    from {G}.mart_volume_anomalies where is_anomaly order by activity_date desc, abs(z_score) desc""")

# ---------------------------------------------------------------- header + filters
st.title("City 311 service requests")
mode = "Live: Snowflake" if be.name == "snowflake" else "Demo data: DuckDB fixtures"
st.caption(f"NYC + Boston · data through **{as_of:%b %d, %Y}** · {mode} · "
           "Airflow → S3 → Snowflake → dbt → Claude")

f1, f2, f3 = st.columns([2, 2, 3])
cities = f1.multiselect("Cities", ["NYC", "Boston"], default=["NYC", "Boston"])
days = f2.segmented_control("Time range", [30, 60, 90], default=60, format_func=lambda d: f"{d} days")
categories = sorted(volume["standard_category"].dropna().unique())
picked = f3.multiselect("Categories", categories, placeholder="All categories")
cities = cities or ["NYC", "Boston"]
days = days or 60

start = pd.Timestamp(as_of) - pd.Timedelta(days=days - 1)
# as_of is the last complete day; the provisional newest day is left out of the charts
vol = volume[(volume["activity_date"] >= start) & (volume["activity_date"] <= pd.Timestamp(as_of))
             & volume["city"].isin(cities)]
if picked:
    vol = vol[vol["standard_category"].isin(picked)]

# ---------------------------------------------------------------- AI report
if data.table_exists(AI, "daily_insights"):
    latest = data.query(f"select * from {AI}.daily_insights order by generated_at desc limit 1")
    if not latest.empty:
        row = latest.iloc[0]
        report = row["insight"] if isinstance(row["insight"], dict) else json.loads(row["insight"])
        label, icon, box = SEVERITY.get(row["severity"], SEVERITY["info"])
        box(f"**{label}: {row['headline']}**\n\n{row['summary']}", icon=icon)
        if report.get("findings"):
            with st.expander(f"AI findings ({len(report['findings'])}) · model {row['model'] or 'none'}"):
                for fnd in report["findings"]:
                    st.markdown(
                        f"**{fnd['title']}** · {fnd['city']} · cause: {fnd['likely_cause'].replace('_', ' ')} "
                        f"({fnd['confidence']} confidence)\n\n{fnd['explanation']}\n\n*Next:* {fnd['recommended_action']}"
                    )
                st.caption("Anomalies are detected in SQL (mart_volume_anomalies); Claude only explains them, "
                           "and findings citing undetected anomalies are discarded.")
else:
    st.info("No AI report yet. The transform DAG writes one daily "
            "(or run scripts/run_insights_local.py in demo mode).", icon=":material/smart_toy:")

# ---------------------------------------------------------------- KPI tiles
cols = st.columns(len(cities))
for col, city in zip(cols, cities, strict=True):
    k = kpis[kpis["city"] == city]
    if k.empty:
        continue
    k = k.iloc[0]
    with col.container(border=True):
        st.markdown(f"**{city}**")
        a, b, c = st.columns(3)
        delta = f"{k['pct_change_7d']:+.0%} vs prior 7d" if pd.notna(k["pct_change_7d"]) else None
        a.metric("Requests, last 7 days", f"{int(k['requests_last_7d']):,}", delta, delta_color="off")
        b.metric("Median hours to close", f"{k['median_resolution_hours_28d']:.0f}"
                 if pd.notna(k["median_resolution_hours_28d"]) else "n/a", help="Last 28 days")
        c.metric("Closed on time", f"{k['pct_on_time_28d']:.0%}" if pd.notna(k["pct_on_time_28d"]) else "n/a",
                 help="Against the city's own target. Boston publishes this; NYC does not.")

# ---------------------------------------------------------------- trend
st.subheader("Requests opened per day")
daily = vol.groupby(["activity_date", "city"], as_index=False)["opened_count"].sum()


def city_trend(df: pd.DataFrame, city: str) -> alt.LayerChart:
    """One panel per city on its own axis: NYC handles ~10x Boston's volume (no dual axis)."""
    hover = alt.selection_point(fields=["activity_date"], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(df).encode(
        x=alt.X("activity_date:T", title=None),
        y=alt.Y("opened_count:Q", title="Requests opened"),
    )
    return alt.layer(
        base.mark_line(strokeWidth=2, color=COLORS[city]),
        base.mark_point(size=80, opacity=0).add_params(hover),
        base.mark_point(size=80, filled=True, color=COLORS[city]).encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[alt.Tooltip("activity_date:T", title="Date"),
                     alt.Tooltip("opened_count:Q", title="Opened", format=",")],
        ),
        base.mark_rule(strokeWidth=1, color="#8a8984").transform_filter(hover),
    ).properties(height=180, title=alt.Title(city, anchor="start", fontSize=14, color=COLORS[city]))


for city in [c for c in ["NYC", "Boston"] if c in cities]:
    st.altair_chart(city_trend(daily[daily["city"] == city], city), width="stretch")

# ---------------------------------------------------------------- Boston migration
if "Boston" in cities:
    st.subheader("Boston's CRM migration")
    st.caption("Case types move from the legacy CRM (12-digit IDs) to the new one (BCS- IDs). "
               "A legacy drop matched by a new-system rise is the migration, not a change in demand.")
    mig = vol[vol["city"] == "Boston"].groupby(["activity_date", "source_system"], as_index=False)["opened_count"].sum()
    mig["system"] = mig["source_system"].map(SYSTEM_LABELS)
    last = mig.sort_values("activity_date").groupby("system").tail(1)
    lines = alt.Chart(mig).mark_line(strokeWidth=2).encode(
        x=alt.X("activity_date:T", title=None),
        y=alt.Y("opened_count:Q", title="Requests opened"),
        color=alt.Color("system:N", scale=SYSTEM_SCALE, legend=alt.Legend(title=None, orient="top")),
        tooltip=[alt.Tooltip("activity_date:T", title="Date"), alt.Tooltip("system:N", title="System"),
                 alt.Tooltip("opened_count:Q", title="Opened", format=",")],
    )
    labels = alt.Chart(last).mark_text(align="left", dx=6, fontSize=12).encode(
        x="activity_date:T", y="opened_count:Q", text="system:N",
        color=alt.value("#52514e" if not DARK else "#c3c2b7"),
    )
    st.altair_chart((lines + labels).properties(height=240), width="stretch")
    with st.expander("Table view"):
        st.dataframe(mig.pivot_table(index="activity_date", columns="system", values="opened_count", fill_value=0)
                     .sort_index(ascending=False), width="stretch")

# ---------------------------------------------------------------- category comparison
st.subheader("Hours to close, shared categories")
st.caption(f"Median hours from report to close, last {int(perf['window_days'].max())} days. "
           "Only categories both cities report, mapped through seeds/category_map.csv.")
p = perf[perf["city"].isin(cities) & (perf["standard_category"] != "Other")]
if picked:
    p = p[p["standard_category"].isin(picked)]
shared = p.groupby("standard_category")["city"].nunique()
p = p[p["standard_category"].isin(shared[shared == len(cities)].index)]
if p.empty:
    st.write("No shared categories for this selection.")
else:
    order = p.groupby("standard_category")["median_resolution_hours"].max().sort_values(ascending=False).index.tolist()
    bars = alt.Chart(p).mark_bar(cornerRadiusEnd=4, height={"band": 0.8}).encode(
        y=alt.Y("standard_category:N", sort=order, title=None, axis=alt.Axis(labelLimit=220)),
        yOffset=alt.YOffset("city:N", sort=["NYC", "Boston"]),
        x=alt.X("median_resolution_hours:Q", title="Median hours to close", axis=alt.Axis(tickCount=8)),
        color=alt.Color("city:N", scale=CITY_SCALE, legend=alt.Legend(title=None, orient="top")),
        tooltip=["city:N", "standard_category:N",
                 alt.Tooltip("median_resolution_hours:Q", title="Median hours", format=".1f"),
                 alt.Tooltip("p90_resolution_hours:Q", title="90th pct hours", format=".1f"),
                 alt.Tooltip("requests:Q", title="Requests", format=",")],
    ).properties(height=alt.Step(14))
    st.altair_chart(bars, width="stretch")

# ---------------------------------------------------------------- anomalies + backlog
left, right = st.columns(2)
with left:
    st.subheader("Flagged anomalies")
    a = anomalies[anomalies["city"].isin(cities)].head(30).copy()
    a["change"] = a["pct_change_vs_baseline"].map(lambda v: f"{v:+.0%}" if pd.notna(v) else "")
    st.dataframe(
        a[["activity_date", "city", "series_type", "series_value", "direction", "request_count",
           "baseline_mean", "z_score", "change"]],
        hide_index=True, width="stretch",
        column_config={"activity_date": st.column_config.DateColumn("date", format="YYYY-MM-DD"),
                       "baseline_mean": st.column_config.NumberColumn("baseline", format="%.0f"),
                       "z_score": st.column_config.NumberColumn("z", format="%.1f"),
                       "request_count": st.column_config.NumberColumn("requests")},
    )
with right:
    st.subheader("Open backlog")
    b = backlog[backlog["city"].isin(cities)].sort_values("open_requests", ascending=False)
    st.dataframe(b[["city", "standard_category", "open_requests", "open_over_7_days", "open_over_30_days",
                    "median_age_days"]], hide_index=True, width="stretch")

# ---------------------------------------------------------------- downloads
st.subheader("Download the gold tables")
st.caption("The same CSVs feed the Tableau Public version of this dashboard.")
dl = st.columns(3)
for i, (name, sql) in enumerate(export_queries(G).items()):
    df = data.query(sql)
    dl[i % 3].download_button(f"{name}.csv ({len(df):,} rows)", df.to_csv(index=False).encode(),
                              file_name=f"{name}.csv", mime="text/csv", width="stretch")

st.caption("Source: NYC Open Data (311 Service Requests) and Analyze Boston (311 Service Requests, "
           "legacy and new system). Built by Chima Ojini · github.com/CHARLESojini/city311-pipeline")
