import streamlit as st
import pandas as pd
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
from google.cloud import bigquery

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- PAGE SETUP ----------------
st.set_page_config(page_title="Data Quality Controls", layout="wide")
st.title("🛡️ Data Quality Controls Assistant")
st.markdown("Welcome! Let's walk through step-by-step to generate your data quality report.")

# ---------------- HELPERS ----------------
def reset_session():
    st.session_state.clear()

@st.cache_data(ttl=600)
def cached_query(query):
    return bq_agent.execute(query)

@st.cache_data(ttl=600)
def cached_query_with_config(query, job_config):
    return bq_agent.execute_with_config(query, job_config)

# ---------------- INIT ----------------
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STEP 1: Control Type ----------------
if "control_type" not in st.session_state:
    st.subheader("🧩 Step 1: Choose Control Type")
    control_type = st.selectbox("Which data quality control would you like to run?", ["Completeness"])
    if st.button("Next ➡️"):
        st.session_state["control_type"] = control_type
        st.rerun()
    st.stop()

# ---------------- STEP 2: Product ----------------
if "selected_product" not in st.session_state:
    st.subheader("🛠️ Step 2: Select Product")
    product_df = cached_query("SELECT DISTINCT product_name FROM `telecom-data-lake.gibantillia.billing_products`")
    product_list = sorted(product_df['product_name'].dropna().tolist())

    selected_product = st.selectbox("Please choose a product:", product_list)
    col1, col2 = st.columns(2)
    with col1:
        if st.button("⬅️ Back"):
            reset_session()
            st.rerun()
    with col2:
        if st.button("Next ➡️"):
            st.session_state["selected_product"] = selected_product
            st.rerun()
    st.stop()

# ---------------- STEP 3: Confirm ----------------
if "confirmed" not in st.session_state:
    st.subheader("✅ Step 3: Confirm Selection")
    st.markdown(f"You've chosen **{st.session_state['control_type']}** control for **{st.session_state['selected_product']}**.")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔁 Start Over"):
            reset_session()
            st.rerun()
    with col2:
        if st.button("🚀 Confirm and Run"):
            st.session_state["confirmed"] = True
            st.rerun()
    st.stop()

# ---------------- STEP 4: Run Report ----------------
st.success(f"🚀 Running {st.session_state['control_type']} control for **{st.session_state['selected_product']}**...")
selected_product = st.session_state["selected_product"]

with st.spinner("Fetching data and generating completeness metrics..."):
    accounts = cached_query("SELECT * FROM `telecom-data-lake.o_siebel.siebel_accounts`")
    assets = cached_query("SELECT * FROM `telecom-data-lake.o_siebel.siebel_assets`")
    orders = cached_query("SELECT * FROM `telecom-data-lake.o_siebel.siebel_orders`")
    billing_accounts = cached_query("SELECT * FROM `telecom-data-lake.gibantillia.billing_accounts`")

    query = """
    SELECT * FROM `telecom-data-lake.gibantillia.billing_products`
    WHERE product_name = @product_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("product_name", "STRING", selected_product)]
    )
    billing_products = cached_query_with_config(query, job_config)

# ---------------- SAFE RENAMING ----------------
def safe_rename(df, rename_map):
    cols = set(df.columns)
    valid = {k: v for k, v in rename_map.items() if k in cols}
    return df.rename(columns=valid)

accounts = safe_rename(accounts, {"account_id": "siebel_account_id"})
assets = safe_rename(assets, {"account_id": "siebel_asset_account_id", "service_number": "siebel_service_number"})
orders = safe_rename(orders, {"account_id": "siebel_order_account_id"})
billing_accounts = safe_rename(billing_accounts, {
    "account_id": "billing_account_siebel_account_id",
    "billing_account_id": "billing_account_id_bacc",
    "status": "billing_account_status",
    "service_number": "billing_service_number"
})
billing_products = safe_rename(billing_products, {"billing_account_id": "billing_account_id_bp"})

# ---------------- MERGE ----------------
merged = (
    billing_products.merge(billing_accounts, left_on="billing_account_id_bp", right_on="billing_account_id_bacc", how="left")
    .merge(accounts, left_on="billing_account_siebel_account_id", right_on="siebel_account_id", how="left")
    .merge(assets, left_on=["asset_id", "billing_service_number"], right_on=["asset_id", "siebel_service_number"], how="left")
    .merge(orders, left_on=["asset_id", "siebel_account_id"], right_on=["asset_id", "siebel_order_account_id"], how="left", suffixes=("", "_order"))
)
merged = merged.loc[:, ~merged.columns.duplicated()]

for col in ["billing_service_number", "siebel_service_number"]:
    if col in merged.columns:
        merged[col] = merged[col].astype(str).str.replace(",", "", regex=False)

# ---------------- KPIs ----------------
merged["service_no_bill"] = (merged.get("asset_status", "") == "Active") & (merged.get("billing_account_status", "") != "Active")
merged["no_service_bill"] = (merged.get("asset_status", "") != "Active") & (merged.get("billing_account_status", "") == "Active")

def classify_kpi(row):
    if row.get("asset_status") == "Active" and row.get("billing_account_status") == "Active":
        return "Happy Path"
    elif row.get("service_no_bill"):
        return "Service No Bill"
    elif row.get("no_service_bill"):
        return "Bill No Service"
    return "DI Issue"

merged["KPI"] = merged.apply(classify_kpi, axis=1)

result_df = merged[[
    "billing_service_number", "siebel_service_number", "siebel_account_id",
    "asset_id", "product_name", "asset_status", "billing_account_status",
    "KPI", "service_no_bill", "no_service_bill"
]].drop_duplicates()

# ---------------- KPI SUMMARY ----------------
st.subheader("🧩 Completeness Summary")
total = len(result_df)
happy_path = (result_df["KPI"] == "Happy Path").sum()
service_no_bill = (result_df["KPI"] == "Service No Bill").sum()
no_service_bill = (result_df["KPI"] == "Bill No Service").sum()

completeness_pct = round((happy_path / total) * 100, 2) if total > 0 else 0.0

st.metric("🧾 Total Records", f"{total:,}")
c1, c2, c3 = st.columns(3)
with c1: st.metric("✅ Happy Path", f"{happy_path:,}")
with c2: st.metric("⚠️ Service No Bill", f"{service_no_bill:,}")
with c3: st.metric("🚫 Bill No Service", f"{no_service_bill:,}")
st.metric("📈 Happy Path (%)", f"{completeness_pct}%")

# ---------------- DETAIL TABLE ----------------
st.subheader("📋 Completeness Report Details")
st.dataframe(result_df.head(500), use_container_width=True)
st.caption(f"Showing up to 500 of {total:,} records")

csv = result_df.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Download Full Report (CSV)", data=csv,
    file_name=f"{selected_product}_completeness_report.csv", mime="text/csv")

# ---------------- STEP 5: Exception Analysis ----------------
st.markdown("---")
st.subheader("Next Step")
if "show_exceptions" not in st.session_state:
    choice = st.radio("Investigate top 10 accounts with exceptions?", ["No", "Yes"], horizontal=True)
    if choice == "Yes":
        st.session_state["show_exceptions"] = True
        st.rerun()
    else:
        st.info("You can restart anytime to choose a different control or product.")
        st.stop()

# ---------------- EXCEPTIONS ----------------
st.subheader("🔍 Investigate Exceptions")
issue_type = st.radio("Select the issue type to explore:", ["Service No Bill", "Bill No Service"], horizontal=True)
filtered = result_df[result_df["KPI"] == issue_type]

if len(filtered) == 0:
    st.warning(f"No records found for **{issue_type}** issues.")
else:
    st.markdown(f"### 📋 Detailed {issue_type} Records (Top 10)")
    st.dataframe(filtered.head(10), use_container_width=True)

    top_accounts = (
        filtered.groupby(["siebel_account_id", "billing_service_number"])
        .size()
        .reset_index(name="exception_count")
        .sort_values("exception_count", ascending=False)
        .head(10)
    )
    st.markdown(f"### 📊 Top 10 Accounts with Most {issue_type} Exceptions")
    st.dataframe(top_accounts, use_container_width=True)

    issue_csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(f"⬇️ Download {issue_type} Records",
        data=issue_csv,
        file_name=f"{selected_product}_{issue_type.replace(' ', '_').lower()}_records.csv",
        mime="text/csv")
# ---------------- FINAL: Restart Option ----------------
st.markdown("---")
st.subheader("🔁 Run Another Control")

if st.button("🏠 Back to Step 1 (Choose Control Type)"):
    reset_session()
    st.rerun()
