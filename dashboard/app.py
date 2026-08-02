import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from src.query.duck_db import (
    get_delivery_summary,
    get_top_categories,
    get_shipping_mode_summary,
    get_delivery_performance,
    get_supplier_performance,
    get_shipping_analysis,
    get_forecast_results,
    get_forecast_accuracy_summary,
)

st.set_page_config(
    page_title="Supply Chain Analytics",
    page_icon="🚚",
    layout="wide",
)

st.title("Supply Chain Analytics Dashboard")
st.caption("DataCo Supply Chain Dataset — 2015 to 2018")

# ── Data ──────────────────────────────────────────────────────────────────────
delivery_summary    = get_delivery_summary()
top_categories      = get_top_categories(10)
shipping_summary    = get_shipping_mode_summary()
delivery_full       = get_delivery_performance()
supplier_full       = get_supplier_performance()
shipping_full       = get_shipping_analysis()

# ── KPI Row ───────────────────────────────────────────────────────────────────
st.subheader("Overview")
col1, col2, col3, col4 = st.columns(4)

total_orders      = int(delivery_summary["total_orders"].sum())
total_late        = int(delivery_summary["late_deliveries"].sum())
overall_late_rate = round(total_late / total_orders * 100, 2)
best_mode         = shipping_summary.loc[shipping_summary["late_rate_pct"].idxmin(), "shipping_mode"]

col1.metric("Total Orders",        f"{total_orders:,}")
col2.metric("Late Deliveries",     f"{total_late:,}")
col3.metric("Overall Late Rate",   f"{overall_late_rate}%")
col4.metric("Best Shipping Mode",  best_mode)

st.divider()

# ── Delivery Performance ──────────────────────────────────────────────────────
st.subheader("Delivery Performance")

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(
        delivery_summary,
        x="market",
        y="late_rate_pct",
        color="late_rate_pct",
        color_continuous_scale="Reds",
        title="Late Delivery Rate by Market (%)",
        labels={"late_rate_pct": "Late Rate (%)", "market": "Market"},
    )
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(
        delivery_full,
        x="order_region",
        y="total_orders",
        color="late_delivery_rate_pct",
        color_continuous_scale="Reds",
        title="Orders by Region (colour = Late Rate %)",
        labels={
            "order_region": "Region",
            "total_orders": "Total Orders",
            "late_delivery_rate_pct": "Late Rate (%)"
        },
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Supplier Performance ──────────────────────────────────────────────────────
st.subheader("Supplier Performance")

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(
        top_categories,
        x="total_revenue",
        y="category_name",
        orientation="h",
        color="profit_margin_pct",
        color_continuous_scale="Greens",
        title="Top 10 Categories by Revenue",
        labels={
            "total_revenue": "Total Revenue ($)",
            "category_name": "Category",
            "profit_margin_pct": "Margin (%)"
        },
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.scatter(
        supplier_full,
        x="total_revenue",
        y="profit_margin_pct",
        size="total_orders",
        color="department_name",
        hover_name="category_name",
        title="Revenue vs Profit Margin by Category",
        labels={
            "total_revenue": "Total Revenue ($)",
            "profit_margin_pct": "Profit Margin (%)",
            "department_name": "Department"
        },
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Shipping Analysis ─────────────────────────────────────────────────────────
st.subheader("Shipping Analysis")

col1, col2 = st.columns(2)

with col1:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Late Rate %",
        x=shipping_summary["shipping_mode"],
        y=shipping_summary["late_rate_pct"],
        marker_color="crimson"
    ))
    fig.add_trace(go.Bar(
        name="On Time Rate %",
        x=shipping_summary["shipping_mode"],
        y=shipping_summary["on_time_rate_pct"],
        marker_color="steelblue"
    ))
    fig.update_layout(
        barmode="group",
        title="Late vs On-Time Rate by Shipping Mode",
        xaxis_title="Shipping Mode",
        yaxis_title="Rate (%)",
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(
        shipping_summary,
        x="shipping_mode",
        y=["avg_days_real", "avg_days_scheduled"],
        barmode="group",
        title="Actual vs Scheduled Shipping Days by Mode",
        labels={
            "shipping_mode": "Shipping Mode",
            "value": "Days",
            "variable": "Type"
        },
    )
    newnames = {"avg_days_real": "Actual Days", "avg_days_scheduled": "Scheduled Days"}
    fig.for_each_trace(lambda t: t.update(name=newnames[t.name]))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Raw Data Tables ───────────────────────────────────────────────────────────
st.subheader("Raw Data")

tab1, tab2, tab3 = st.tabs([
    "Delivery Performance",
    "Supplier Performance",
    "Shipping Analysis"
])

with tab1:
    st.dataframe(delivery_full, use_container_width=True)

with tab2:
    st.dataframe(supplier_full, use_container_width=True)

with tab3:
    st.dataframe(shipping_full, use_container_width=True)

st.divider()

# ── Demand Forecasting ────────────────────────────────────────────────────────
st.subheader("Demand Forecasting")
st.caption(
    "Moving Average (4w / 8w / 12w) vs Prophet — trained on 2015–2016, tested on 2017."
)

# Model accuracy summary
accuracy_summary = get_forecast_accuracy_summary()

col1, col2, col3, col4 = st.columns(4)
models_ordered = accuracy_summary.sort_values("avg_wmape")
best = models_ordered.iloc[0]
col1.metric("Best Model",       best["model"])
col2.metric("Best WMAPE",       f"{best['avg_wmape']:.1f}%")
col3.metric("Best RMSE",        f"{best['avg_rmse']:.1f}")
col4.metric("Test Weeks",       f"{int(models_ordered['total_test_weeks'].max()):,}")

st.subheader("Model Accuracy Comparison (Test Set)")
st.dataframe(
    accuracy_summary.rename(columns={
        "model": "Model", "avg_rmse": "Avg RMSE", "avg_mae": "Avg MAE",
        "avg_wmape": "Avg WMAPE (%)", "avg_bias": "Avg Bias",
        "total_test_weeks": "Test Weeks",
    }),
    use_container_width=True,
    hide_index=True,
)

# Forecast vs Actual chart
st.subheader("Forecast vs Actual — Weekly Demand")

all_forecasts = get_forecast_results()
markets    = sorted(all_forecasts["market"].unique())
sel_market = st.selectbox("Market", markets, key="fc_market")

categories = sorted(
    all_forecasts.loc[all_forecasts["market"] == sel_market, "category_name"].unique()
)
sel_category = st.selectbox("Category", categories, key="fc_category")

fc_filtered = all_forecasts[
    (all_forecasts["market"] == sel_market) &
    (all_forecasts["category_name"] == sel_category)
].sort_values("week")

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=fc_filtered["week"], y=fc_filtered["actual_quantity"],
    mode="lines", name="Actual", line=dict(color="steelblue", width=2),
))
for col_name, label, colour in [
    ("ma_4w",  "MA 4w",  "orange"),
    ("ma_8w",  "MA 8w",  "green"),
    ("ma_12w", "MA 12w", "purple"),
    ("prophet_forecast", "Prophet", "crimson"),
]:
    if col_name in fc_filtered.columns:
        fig.add_trace(go.Scatter(
            x=fc_filtered["week"], y=fc_filtered[col_name],
            mode="lines", name=label,
            line=dict(dash="dash", color=colour, width=1.5),
        ))

# Shade train/test regions
if "split" in fc_filtered.columns and len(fc_filtered) > 0:
    test_start = fc_filtered.loc[fc_filtered["split"] == "test", "week"].min()
    if test_start is not None and not fc_filtered.empty:
        fig.add_vrect(
            x0=test_start, x1=fc_filtered["week"].max(),
            fillcolor="lightgrey", opacity=0.2, layer="below",
            annotation_text="Test period", annotation_position="top left",
        )

fig.update_layout(
    title=f"{sel_market} — {sel_category}",
    xaxis_title="Week",
    yaxis_title="Units",
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
)
st.plotly_chart(fig, use_container_width=True)
