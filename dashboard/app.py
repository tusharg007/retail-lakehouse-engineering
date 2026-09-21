import os
import pandas as pd
import streamlit as st
from pyhive import hive

st.set_page_config(page_title="Retail Lakehouse", layout="wide")
st.title("Retail Lakehouse metrics")
st.caption("Live Spark SQL output. Values are derived from the latest successful dbt build; a failed query is shown rather than replaced with sample data.")

@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    connection = hive.Connection(host=os.getenv("SPARK_THRIFT_HOST", "spark-thrift"), port=int(os.getenv("SPARK_THRIFT_PORT", "10000")), username="retail", auth="NOSASL")
    return pd.read_sql(sql, connection)

try:
    data = query("SELECT order_status, sum(gross_line_amount) gross_line_amount, sum(completed_line_amount) completed_line_amount, sum(order_count) order_count FROM gold.mart_daily_sales GROUP BY order_status ORDER BY order_status")
    st.dataframe(data, use_container_width=True, hide_index=True)
    totals = query("SELECT sum(gross_order_amount) gross_order_amount, count(*) order_count FROM gold.fact_orders")
    left, right = st.columns(2)
    left.metric("Gross source order amount", f"{totals.iloc[0, 0]:,.2f}")
    right.metric("Orders", f"{int(totals.iloc[0, 1]):,}")
except Exception as error:
    st.error(f"Live Spark query unavailable: {error}")
