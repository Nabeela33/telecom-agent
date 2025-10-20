import streamlit as st
import pandas as pd
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- LOAD MAPPINGS ----------------
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Telecom Data Quality & Controls")
st.markdown("Select a control type and product to generate a completeness report.")

# ---------------- SIDEBAR FILTERS ----------------
control_type = st.sidebar.selectbox(
    "Select Control Type:",
    ["Completeness"]
)

# Fetch distinct product names from billing_products
billing_products_df = bq_agent.execute("SELECT DISTINCT product_name FROM `telecom-data-lake.gibantillia.billing_products`")
product_name = st.sidebar.selectbox(
    "Select Product:",
    billing_products_df['product_name'].tolist()
)

# ---------------- CONFIRM SELECTION ----------------
if st.button("Confirm Selection"):
    st.subheader(f"Running {control_type} Control for product: {product_name}")

    try:
        # ---------------- COMPLETENESS LOGIC ----------------
        # Fetch tables
        assets_df = bq_agent.execute("SELECT account_id, asset_id, asset_status FROM `telecom-data-lake.o_siebel.siebel_assets`")
        accounts_df = bq_agent.execute("SELECT account_id FROM `telecom-data-lake.o_siebel.siebel_accounts`")
        billing_accounts_df = bq_agent.execute("SELECT billing_account_id, account_id, status AS billing_status FROM `telecom-data-lake.gibantillia.billing_accounts`")
        billing_products_df = bq_agent.execute(f"""
            SELECT billing_account_id, asset_id, status AS billing_status, product_name
            FROM `telecom-data-lake.gibantillia.billing_products`
            WHERE product_name = '{product_name}'
        """)

        # Join billing_products → billing_accounts
        billing_full_df = billing_products_df.merge(
            billing_accounts_df,
            on="billing_account_id",
            how="left",
            suffixes=('', '_acct')
        )

        # Join assets
        full_df = billing_full_df.merge(
            assets_df,
            on="asset_id",
            how="left"
        )

        # Merge with accounts to get only valid accounts
        full_df = full_df.merge(
            accounts_df,
            on="account_id",
            how="left"
        )

        # KPI Calculations
        full_df['service_no_bill'] = ((full_df['asset_status'] == 'Active') & (full_df['billing_status'] != 'Active'))
        full_df['no_service_bill'] = ((full_df['asset_status'] != 'Active') & (full_df['billing_status'] == 'Active'))

        kpi_service_no_bill = full_df['service_no_bill'].sum()
        kpi_no_service_bill = full_df['no_service_bill'].sum()

        # ---------------- DISPLAY RESULTS ----------------
        st.success("✅ Completeness Calculation Done")
        st.subheader("Key Metrics")
        st.write(f"**Service but No Bill:** {kpi_service_no_bill}")
        st.write(f"**No Service but Bill:** {kpi_no_service_bill}")

        st.subheader("Detailed Results")
        with st.expander("View full table"):
            st.dataframe(full_df)

        # Download option
        csv = full_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{product_name}_completeness_report.csv",
            mime='text/csv'
        )

    except Exception as e:
        st.error(f"❌ Error: {e}")
