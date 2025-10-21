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
st.sidebar.info("Select control type and product to run the completeness report at service number level.")

siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📞 Service-Level Data Quality Controls")
st.markdown("Analyze completeness at **service number** level between Siebel and Antillia datasets.")

# ---------------- SIDEBAR ----------------
control_type = st.sidebar.selectbox("Select Control Type", ["Completeness"])
product_df = bq_agent.execute("SELECT DISTINCT product_name FROM `telecom-data-lake.gibantillia.billing_products`")
product_list = product_df["product_name"].tolist()
selected_product = st.sidebar.selectbox("Select Product", product_list)

if st.sidebar.button("Confirm Selection"):
    st.session_state["confirmed"] = True

if st.session_state.get("confirmed", False):
    st.success(f"🚀 Running {control_type} control for product: **{selected_product}**")

    # ---------------- FETCH DATA ----------------
    siebel_assets = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_assets`")
    billing_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_accounts`")
    billing_products = bq_agent.execute(f"""
        SELECT * FROM `telecom-data-lake.gibantillia.billing_products`
        WHERE product_name = '{selected_product}'
    """)

    # ---------------- MERGE ON SERVICE NUMBER ----------------
    merged = billing_products.merge(
        billing_accounts,
        left_on="billing_account_id",
        right_on="billing_account_id",
        how="left"
    ).merge(
        siebel_assets,
        left_on="service_number",
        right_on="service_number",
        how="left",
        suffixes=("_bill", "_asset")
    )

    # ---------------- COMPLETENESS KPIS ----------------
    merged["service_no_bill"] = (merged["asset_status"] == "Active") & (merged["status_bill"] != "Active")
    merged["no_service_bill"] = (merged["asset_status"] != "Active") & (merged["status_bill"] == "Active")

    def classify_kpi(row):
        if row["asset_status"] == "Active" and row["status_bill"] == "Active":
            return "Happy Path"
        elif row["service_no_bill"]:
            return "Service No Bill"
        elif row["no_service_bill"]:
            return "Bill No Service"
        else:
            return "Data Issue"

    merged["KPI"] = merged.apply(classify_kpi, axis=1)

    # ---------------- RESULTS ----------------
    result_df = merged[[
        "service_number",
        "product_name",
        "asset_status",
        "status_bill",
        "KPI"
    ]].drop_duplicates()

    # ---------------- KPI SUMMARY ----------------
    st.subheader("📊 Completeness Summary by Service Number")

    total = len(result_df)
    happy_path = (result_df["KPI"] == "Happy Path").sum()
    service_no_bill = (result_df["KPI"] == "Service No Bill").sum()
    no_service_bill = (result_df["KPI"] == "Bill No Service").sum()
    completeness_pct = round((happy_path / total) * 100, 2) if total > 0 else 0.0

    c1, c2 = st.columns(2)
    c1.metric("📞 Total Services", f"{total:,}")
    c2.metric("✅ Completeness (%)", f"{completeness_pct}%")

    c3, c4, c5 = st.columns(3)
    c3.metric("Happy Path", f"{happy_path:,}")
    c4.metric("Service No Bill", f"{service_no_bill:,}")
    c5.metric("Bill No Service", f"{no_service_bill:,}")

    # ---------------- DETAILED TABLE ----------------
    st.subheader("📋 Completeness Report Details (Service Level)")
    st.dataframe(result_df)

    # ---------------- DOWNLOAD OPTION ----------------
    csv = result_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download CSV",
        data=csv,
        file_name=f"{selected_product}_service_level_completeness.csv",
        mime="text/csv"
    )
