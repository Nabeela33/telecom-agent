import streamlit as st
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
import pandas as pd

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- LOAD MAPPINGS ----------------
st.sidebar.title("Configuration")
st.sidebar.info("Select control type and product to run the completeness report.")

siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Data Quality Controls")
st.markdown("Select a product and control type to generate the report")

# ---------------- SIDEBAR ----------------
control_type = st.sidebar.selectbox("Select Control Type", ["Completeness"])

# Dynamically fetch product list
product_df = bq_agent.execute("SELECT DISTINCT product_name FROM `telecom-data-lake.gibantillia.billing_products`")
product_list = product_df['product_name'].tolist()
selected_product = st.sidebar.selectbox("Select Product", product_list)

if st.sidebar.button("Confirm Selection"):
    st.session_state['confirmed'] = True

if st.session_state.get('confirmed', False):
    st.success(f"Running {control_type} control for product: **{selected_product}**")

    # ---------------- FETCH DATA ----------------
    accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_accounts`")
    assets = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_assets`")
    orders = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_orders`")
    billing_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_accounts`")
    billing_products = bq_agent.execute(f"""
        SELECT * FROM `telecom-data-lake.gibantillia.billing_products`
        WHERE product_name = '{selected_product}'
    """)

    # ---------------- SAFE RENAMING ----------------
    # Give each dataset unique identifiers to avoid duplicate column names
    if 'account_id' in accounts.columns:
        accounts = accounts.rename(columns={"account_id": "siebel_account_id"})
    if 'account_id' in assets.columns:
        assets = assets.rename(columns={"account_id": "siebel_asset_account_id", "status": "asset_status"})
    if 'account_id' in orders.columns:
        orders = orders.rename(columns={"account_id": "siebel_order_account_id"})
    if 'account_id' in billing_accounts.columns:
        billing_accounts = billing_accounts.rename(columns={
            "account_id": "billing_account_siebel_account_id",
            "billing_account_id": "billing_account_id_bacc",
            "status": "billing_account_status"
        })
    if 'billing_account_id' in billing_products.columns:
        billing_products = billing_products.rename(columns={"billing_account_id": "billing_account_id_bp"})

    # ---------------- MERGE LOGIC ----------------
    merged = billing_products.merge(
        billing_accounts, left_on="billing_account_id_bp", right_on="billing_account_id_bacc", how="left"
    ).merge(
        accounts, left_on="billing_account_siebel_account_id", right_on="siebel_account_id", how="left"
    ).merge(
        assets, on="asset_id", how="left"
    ).merge(
        orders, left_on=["asset_id", "siebel_account_id"], right_on=["asset_id", "siebel_order_account_id"], how="left", suffixes=("", "_order")
    )

    # ---------------- COMPLETENESS KPIS ----------------
    merged['service_no_bill'] = (
        (merged["asset_status"] == "Active") &
        (merged.get("billing_account_status", "") != "Active")
    )

    merged['no_service_bill'] = (
        (merged["asset_status"] != "Active") &
        (merged.get("billing_account_status", "") == "Active")
    )

    result_df = merged[[
        "siebel_account_id",
        "asset_id",
        "product_name",
        "asset_status",
        "billing_account_status",
        "service_no_bill",
        "no_service_bill"
    ]]

    st.subheader("📊 Completeness Report")
    st.dataframe(result_df)

    # ---------------- DOWNLOAD OPTION ----------------
    csv = result_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"{selected_product}_completeness_report.csv",
        mime="text/csv"
    )
