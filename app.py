import streamlit as st
from utils import load_mapping
from bigquery_client import BigQueryAgent
import pandas as pd

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- LOAD MAPPINGS ----------------
st.sidebar.title("Configuration")
st.sidebar.info("Mappings guide table/column usage dynamically.")

siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENT ----------------
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Telecom Product Completeness Control")
st.markdown(
    "Select a control type and product, confirm, and generate the completeness report."
)

# ---------------- SIDEBAR SELECTIONS ----------------
control_types = ["Completeness"]  # Can extend in future
control_type = st.sidebar.selectbox("Select Control Type", control_types)

# Extract distinct product names from Antillia mapping
product_column = antillia_mapping["columns"]["billing_products"]
product_name_col = "product_name" if "product_name" in product_column else product_column[0]

# Query to fetch distinct products dynamically
distinct_products_query = f"""
SELECT DISTINCT {product_name_col} as product_name
FROM `{PROJECT_ID}.gibantillia.billing_products`
ORDER BY product_name
"""
products_df = bq_agent.execute(distinct_products_query)
products = products_df["product_name"].tolist()
selected_product = st.sidebar.selectbox("Select Product", products)

# ---------------- CONFIRMATION ----------------
confirm = st.button("✅ Confirm Selection")

if confirm:
    st.info(f"Running completeness report for product: **{selected_product}**")
    try:
        # ---------------- BUILD JOIN QUERY ----------------
        query = f"""
        SELECT 
            acc.account_id AS siebel_account_id,
            acc.account_name,
            acc.status AS siebel_account_status,
            a.asset_id,
            a.asset_type,
            a.status AS asset_status,
            o.order_id,
            bp.billing_account_id,
            bp.billing_product_id,
            bp.status AS billing_status,
            bp.product_name
        FROM `{PROJECT_ID}.gibantillia.billing_products` bp
        INNER JOIN `{PROJECT_ID}.gibantillia.billing_accounts` bacc
            ON bp.billing_account_id = bacc.billing_account_id
        INNER JOIN `{PROJECT_ID}.o_siebel.siebel_accounts` acc
            ON bacc.account_id = acc.account_id
        INNER JOIN `{PROJECT_ID}.o_siebel.siebel_assets` a
            ON bp.asset_id = a.asset_id
        INNER JOIN `{PROJECT_ID}.o_siebel.siebel_orders` o
            ON bp.order_id = o.order_id
        WHERE bp.product_name = '{selected_product}'
        """

        # ---------------- EXECUTE QUERY ----------------
        df = bq_agent.execute(query)

        if df.empty:
            st.warning("No data found for the selected product.")
        else:
            # ---------------- KPI CALCULATION ----------------
            # Service Active but Billing Not Active → Service No Bill
            df["service_no_bill"] = (
                (df["asset_status"] == "Operational") & (df["billing_status"] != "Active")
            )

            # Asset Not Active but Billing Active → No Service Bill
            df["no_service_bill"] = (
                (df["asset_status"] != "Operational") & (df["billing_status"] == "Active")
            )

            st.subheader("📋 Completeness Report")
            with st.expander("View Detailed Data"):
                st.dataframe(df)

            st.markdown(
                f"**KPI Summary:**<br>"
                f"- Service No Bill: {df['service_no_bill'].sum()}<br>"
                f"- No Service Bill: {df['no_service_bill'].sum()}",
                unsafe_allow_html=True
            )

            # ---------------- DOWNLOAD OPTION ----------------
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"completeness_{selected_product}.csv",
                mime="text/csv",
            )

    except Exception as e:
        st.error(f"❌ Error: {e}")
