import streamlit as st
import pandas as pd
from utils import load_mapping
from bigquery_client import BigQueryAgent

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- LOAD MAPPINGS ----------------
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Data Quality Controls")
st.markdown(
    "Select control type and product to generate a report. "
   # "This will show if services are billed or not based on asset and billing status."
)

# Sidebar filters
st.sidebar.header("Filters")
control_type = st.sidebar.selectbox("Select Control Type", ["Completeness Control"])
product_list_query = """
SELECT DISTINCT product_name
FROM `telecom-data-lake.gibantillia.billing_products`
ORDER BY product_name
"""
product_list = bq_agent.execute(product_list_query)["product_name"].tolist()
selected_product = st.sidebar.selectbox("Select Product", product_list)

# Confirmation
if st.button("Run Completeness Control"):
    if not selected_product:
        st.warning("Please select a product!")
    else:
        st.info(f"Generating completeness report for **{selected_product}**...")

        # ---------------- FETCH DATA ----------------
        siebel_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_accounts`")
        siebel_assets = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_assets`")
        siebel_orders = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_orders`")
        billing_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_accounts`")
        billing_products = bq_agent.execute(
            f"SELECT * FROM `telecom-data-lake.gibantillia.billing_products` WHERE product_name = '{selected_product}'"
        )

        # ---------------- MERGE LOGIC ----------------
        merged = billing_products.merge(
            billing_accounts, left_on="billing_account_id", right_on="billing_account_id", how="left"
        ).rename(columns={"status": "billing_account_status"})

        merged = merged.merge(
            siebel_accounts, left_on="account_id", right_on="account_id", how="left"
        ).rename(columns={"status": "siebel_account_status"})

        merged = merged.merge(
            siebel_assets, left_on="asset_id", right_on="asset_id", how="left"
        ).rename(columns={"status": "asset_status"})

        merged = merged.merge(
            siebel_orders, left_on="order_id", right_on="order_id", how="left"
        ).rename(columns={"status": "order_status"})

        # ---------------- KPI LOGIC ----------------
        merged["service_no_bill"] = (
            (merged["asset_status"] == "Operational") & (merged["billing_account_status"] != "Active")
        )
        merged["no_service_bill"] = (
            (merged["asset_status"] != "Operational") & (merged["billing_account_status"] == "Active")
        )

        # ---------------- DISPLAY RESULTS ----------------
        st.success(f"✅ Completeness report generated! {len(merged)} rows returned.")
        with st.expander("View Full Report"):
            st.dataframe(merged)

        # ---------------- DOWNLOAD OPTION ----------------
        csv = merged.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download report as CSV",
            data=csv,
            file_name=f"{selected_product}_completeness_report.csv",
            mime="text/csv",
        )
