import streamlit as st
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
import pandas as pd

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- INIT ----------------
st.set_page_config(page_title="Data Quality Controls", layout="wide")

# ---------------- LOAD MAPPINGS ----------------
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- HEADER ----------------
st.title("🛡️ Data Quality Controls Assistant")
st.markdown("Welcome! Let's walk through step-by-step to generate your data quality report.")

# Helper to reset
def reset_session():
    for key in ["control_type", "selected_product", "confirmed", "show_exceptions"]:
        if key in st.session_state:
            del st.session_state[key]

# ---------------- STEP 1: Select Control Type ----------------
if "control_type" not in st.session_state:
    st.subheader("🧩 Step 1: Choose Control Type")
    control_type = st.selectbox("Which data quality control would you like to run?", ["Completeness"])
    if st.button("Next ➡️"):
        st.session_state["control_type"] = control_type
        st.rerun()
    st.stop()

# ---------------- STEP 2: Select Product ----------------
if "selected_product" not in st.session_state:
    st.subheader("🛠️ Step 2: Select Product")
    product_df = bq_agent.execute("SELECT DISTINCT product_name FROM `telecom-data-lake.gibantillia.billing_products`")
    product_list = sorted(product_df['product_name'].dropna().tolist())

    selected_product = st.selectbox("Please choose a product:", product_list)

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("⬅️ Back"):
            del st.session_state["control_type"]
            st.rerun()
    with col2:
        if st.button("Next ➡️"):
            st.session_state["selected_product"] = selected_product
            st.rerun()
    st.stop()

# ---------------- STEP 3: Confirm Selection ----------------
if "confirmed" not in st.session_state:
    st.subheader("✅ Step 3: Confirm Selection")
    st.markdown(f"You've chosen to run **{st.session_state['control_type']}** control for product **{st.session_state['selected_product']}**.")
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

# ---------------- STEP 4: Run Completeness Report ----------------
st.success(f"🚀 Running {st.session_state['control_type']} control for **{st.session_state['selected_product']}**...")

selected_product = st.session_state["selected_product"]

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
if 'account_id' in accounts.columns:
    accounts = accounts.rename(columns={"account_id": "siebel_account_id"})

if 'account_id' in assets.columns:
    assets = assets.rename(columns={
        "account_id": "siebel_asset_account_id"
    })
    if 'service_number' in assets.columns:
        assets = assets.rename(columns={"service_number": "siebel_service_number"})

if 'account_id' in orders.columns:
    orders = orders.rename(columns={"account_id": "siebel_order_account_id"})

if 'account_id' in billing_accounts.columns:
    billing_accounts = billing_accounts.rename(columns={
        "account_id": "billing_account_siebel_account_id",
        "billing_account_id": "billing_account_id_bacc",
        "status": "billing_account_status"
    })
    if 'service_number' in billing_accounts.columns:
        billing_accounts = billing_accounts.rename(columns={"service_number": "billing_service_number"})

if 'billing_account_id' in billing_products.columns:
    billing_products = billing_products.rename(columns={"billing_account_id": "billing_account_id_bp"})

# ---------------- MERGE LOGIC ----------------
merged = (
    billing_products.merge(
        billing_accounts, left_on="billing_account_id_bp", right_on="billing_account_id_bacc", how="left"
    )
    .merge(
        accounts, left_on="billing_account_siebel_account_id", right_on="siebel_account_id", how="left"
    )
    .merge(
        assets, left_on=["asset_id", "billing_service_number"], right_on=["asset_id", "siebel_service_number"], how="left"
    )
    .merge(
        orders,
        left_on=["asset_id", "siebel_account_id"],
        right_on=["asset_id", "siebel_order_account_id"],
        how="left",
        suffixes=("", "_order")
    )
)
merged = merged.loc[:, ~merged.columns.duplicated()]

# Clean commas
for col in ["billing_service_number", "siebel_service_number"]:
    if col in merged.columns:
        merged[col] = merged[col].astype(str).str.replace(",", "", regex=False)

# ---------------- KPIs ----------------
merged["service_no_bill"] = (
    (merged.get("asset_status", "") == "Active") &
    (merged.get("billing_account_status", "") != "Active")
)
merged["no_service_bill"] = (
    (merged.get("asset_status", "") != "Active") &
    (merged.get("billing_account_status", "") == "Active")
)

def classify_kpi(row):
    if row.get("asset_status") == "Active" and row.get("billing_account_status") == "Active":
        return "Happy Path"
    elif row.get("service_no_bill"):
        return "Service No Bill"
    elif row.get("no_service_bill"):
        return "Bill No Service"
    else:
        return "DI Issue"

merged["KPI"] = merged.apply(classify_kpi, axis=1)

result_df = merged[[
    "billing_service_number",
    "siebel_service_number",
    "siebel_account_id",
    "asset_id",
    "product_name",
    "asset_status",
    "billing_account_status",
    "KPI",
    "service_no_bill",
    "no_service_bill"
]].drop_duplicates()

# ---------------- KPI SUMMARY ----------------
st.subheader("🧩 Completeness Summary")

total = len(result_df)
happy_path = (result_df["KPI"] == "Happy Path").sum()
service_no_bill = (result_df["KPI"] == "Service No Bill").sum()
no_service_bill = (result_df["KPI"] == "Bill No Service").sum()

completeness_pct = round((happy_path / total) * 100, 2) if total > 0 else 0.0

c1, c2 = st.columns(2)
with c1:
    st.metric("🧾 Total Records", f"{total:,}")
with c2:
    st.metric("📈 Happy Path (%)", f"{completeness_pct} %")

c3, c4, c5 = st.columns(3)
with c3:
    st.metric("✅ Happy Path", f"{happy_path:,}")
with c4:
    st.metric("⚠️ Service No Bill", f"{service_no_bill:,}")
with c5:
    st.metric("🚫 Bill No Service", f"{no_service_bill:,}")

# ---------------- DETAILED REPORT ----------------
st.subheader("📋 Completeness Report Details")
st.dataframe(result_df)

csv = result_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Download Full Report (CSV)",
    data=csv,
    file_name=f"{selected_product}_completeness_report.csv",
    mime="text/csv"
)

# ---------------- STEP 5: Ask for Exception Analysis ----------------
st.markdown("---")
st.subheader("Next Step")

if "show_exceptions" not in st.session_state:
    choice = st.radio(
        "Would you like to investigate top 10 accounts with exceptions?",
        ["No", "Yes"],
        horizontal=True,
        key="investigate_choice"
    )
    if choice == "Yes":
        st.session_state["show_exceptions"] = True
        st.rerun()
    else:
        st.info("You can restart anytime to choose a different control or product.")
        st.stop()

# ---------------- EXCEPTION ANALYSIS ----------------
st.subheader("🔍 Investigate Exceptions")
issue_type = st.radio(
    "Select the issue type to explore:",
    ["Service No Bill", "Bill No Service"],
    horizontal=True
)
filtered = result_df[result_df["KPI"] == issue_type]

if len(filtered) == 0:
    st.warning(f"No records found for **{issue_type}** issues.")
else:
    detailed_view = filtered[[
        "siebel_account_id",
        "billing_service_number",
        "siebel_service_number",
        "asset_status",
        "billing_account_status",
        "product_name",
        "KPI"
    ]]
    st.markdown(f"### 📋 Detailed {issue_type} Records (Top 10)")
    st.dataframe(detailed_view.head(10))

    top_accounts = (
        filtered.groupby(["siebel_account_id", "billing_service_number"])
        .size()
        .reset_index(name="exception_count")
        .sort_values("exception_count", ascending=False)
        .head(10)
    )
    st.markdown(f"### 📊 Top 10 Accounts + Service Numbers with Most **{issue_type}** Exceptions")
    st.dataframe(top_accounts)

    issue_csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"⬇️ Download {issue_type} Records",
        data=issue_csv,
        file_name=f"{selected_product}_{issue_type.replace(' ', '_').lower()}_records.csv",
        mime="text/csv"
    )
