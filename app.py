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

# Extract columns dynamically from mapping
def get_columns(mapping_text, table_name):
    """
    Returns a list of columns for a given table from mapping file text
    """
    cols = []
    lines = mapping_text.split("\n")
    capture = False
    for line in lines:
        line = line.strip()
        if line.startswith(f"==========================") and table_name in line:
            capture = True
            continue
        if capture:
            if line.startswith("Columns:"):
                continue
            elif line.startswith("- "):
                cols.append(line[2:].split(":")[0].strip())
            elif line == "":
                break
    return cols

# ---------------- INIT AGENTS ----------------
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Data Quality Controls")
st.sidebar.title("Filters")

# Control type (future extension)
control_type = st.sidebar.selectbox("Select Control Type", ["Product Completeness"])

# Get product names dynamically from billing_products table
with st.spinner("Fetching product names..."):
    product_names_df = bq_agent.execute("SELECT DISTINCT product_name FROM `telecom-data-lake.gibantillia.billing_products`")
product_name = st.sidebar.selectbox("Select Product", product_names_df['product_name'].tolist())

if st.button("Confirm & Run Control"):
    with st.spinner("Running completeness control..."):
        # ---------------- FETCH DATA ----------------
        siebel_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_accounts`")
        siebel_assets = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_assets`")
        siebel_orders = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_orders`")
        billing_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_accounts`")
        billing_products = bq_agent.execute(f"""
            SELECT * FROM `telecom-data-lake.gibantillia.billing_products`
            WHERE product_name = '{product_name}'
        """)

        # ---------------- JOIN LOGIC ----------------
        merged = billing_products.merge(
            billing_accounts, left_on="billing_account_id", right_on="billing_account_id", suffixes=("", "_ant"))
        merged = merged.merge(
            siebel_accounts, left_on="account_id", right_on="account_id", suffixes=("_ant", "_siebel"))
        merged = merged.merge(
            siebel_assets, left_on="asset_id", right_on="asset_id", suffixes=("", "_asset"))
        merged = merged.merge(
            siebel_orders, left_on="order_id", right_on="order_id", suffixes=("", "_order"))

        # ---------------- KPI LOGIC ----------------
        # If asset active but billing inactive => service_no_bill
        merged["service_no_bill"] = ((merged["status_asset"]=="Operational") & (merged["status_ant"]!="Active"))
        # If asset inactive but billing active => no_service_bill
        merged["no_service_bill"] = ((merged["status_asset"]!="Operational") & (merged["status_ant"]=="Active"))

        # ---------------- RESULTS ----------------
        result_cols = ["account_id", "asset_id", "order_id", "product_name", "service_no_bill", "no_service_bill"]
        st.subheader("Completeness Control Result")
        st.dataframe(merged[result_cols])

        # Downloadable CSV
        csv = merged[result_cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"{product_name}_completeness.csv",
            mime="text/csv"
        )

        st.success("✅ Control completed!")
