import streamlit as st
import pandas as pd
from utils import load_mapping
from bigquery_client import BigQueryAgent

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- INIT AGENTS ----------------
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Telecom Completeness Control")
st.markdown(
    "Select a control type and product to generate the completeness report. "
    "KPIs:\n- **Service but no Bill**: Active asset but inactive billing.\n"
    "- **No Service but Bill**: Inactive asset but active billing."
)

# Sidebar filters
st.sidebar.title("Filters")
control_type = st.sidebar.selectbox("Select Control Type", ["Completeness"])
product_filter = st.sidebar.text_input("Enter Product Name")

if st.sidebar.button("Generate Report"):
    confirm = st.checkbox(f"Confirm to run '{control_type}' control for product: '{product_filter}'")
    
    if confirm:
        with st.spinner("Fetching data from BigQuery..."):
            # Load Siebel tables
            siebel_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_accounts`")
            siebel_assets = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_assets`")
            siebel_orders = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_orders`")

            # Load Antillia tables
            billing_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_accounts`")
            billing_products = bq_agent.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_products`")

        # ---------------- RENAME COLUMNS TO AVOID CONFLICTS ----------------
        siebel_accounts = siebel_accounts.rename(columns={"account_id": "siebel_account_id"})
        siebel_assets = siebel_assets.rename(columns={
            "account_id": "siebel_account_id",
            "asset_id": "siebel_asset_id"
        })
        siebel_orders = siebel_orders.rename(columns={
            "account_id": "siebel_account_id",
            "asset_id": "siebel_asset_id",
            "order_id": "siebel_order_id"
        })
        billing_accounts = billing_accounts.rename(columns={"account_id": "antillia_account_id"})
        billing_products = billing_products.rename(columns={
            "account_id": "antillia_account_id",
            "asset_id": "antillia_asset_id",
            "order_id": "antillia_order_id"
        })

        # ---------------- FILTER PRODUCT ----------------
        if product_filter:
            billing_products = billing_products[billing_products["product_name"].str.contains(product_filter, case=False)]

        # ---------------- JOIN TABLES ----------------
        df = (
            billing_products
            .merge(billing_accounts, on="billing_account_id", how="left")
            .merge(siebel_accounts, left_on="antillia_account_id", right_on="siebel_account_id", how="left")
            .merge(siebel_assets, left_on="antillia_asset_id", right_on="siebel_asset_id", how="left")
        )

        # ---------------- CALCULATE KPIs ----------------
        df["service_no_bill"] = (
            (df["asset_status"] == "Active") &
            (df["status_y"] != "Active")  # billing status
        )

        df["bill_no_service"] = (
            (df["asset_status"] != "Active") &
            (df["status_y"] == "Active")  # billing status
        )

        # ---------------- DISPLAY RESULTS ----------------
        st.success(f"✅ Report generated! {len(df)} rows.")
        st.subheader("Completeness Results")
        with st.expander("View Full Dataset"):
            st.dataframe(df)

        st.subheader("KPIs Summary")
        st.markdown(f"- Service no bill: {df['service_no_bill'].sum()}")
        st.markdown(f"- Bill no service: {df['bill_no_service'].sum()}")

        # ---------------- DOWNLOAD OPTION ----------------
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"{control_type}_report.csv",
            mime="text/csv"
        )
