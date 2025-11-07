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

# ---------------- HELPER: RESET SESSION ----------------
def reset_session():
    """Clears all step-related session state keys to restart the workflow."""
    for key in ["control_type", "selected_product", "confirmed", "show_exceptions"]:
        if key in st.session_state:
            del st.session_state[key]

# ---------------- STEP 1: Select Control Type ----------------
if "control_type" not in st.session_state:
    st.subheader("🧩 Step 1: Choose Control Type")
    control_type = st.selectbox(
        "Which data quality control would you like to run?",
        ["Completeness", "Accuracy"]
    )
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

# ---------------- STEP 4: Run Report ----------------
selected_product = st.session_state["selected_product"]
control_type = st.session_state["control_type"]

st.success(f"🚀 Running {control_type} control for **{selected_product}**...")

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

# ---------------- COMPLETENESS LOGIC ----------------
def is_available(status):
    """Treat both Active and Completed as available statuses."""
    if pd.isna(status):
        return False
    return str(status).strip().lower() in ["active", "completed", "complete"]

merged["service_no_bill"] = (
    merged["asset_status"].apply(is_available)
    & ~merged["billing_account_status"].apply(is_available)
)

merged["no_service_bill"] = (
    ~merged["asset_status"].apply(is_available)
    & merged["billing_account_status"].apply(is_available)
)

def classify_kpi(row):
    asset_ok = is_available(row.get("asset_status"))
    billing_ok = is_available(row.get("billing_account_status"))
    if asset_ok and billing_ok:
        return "Happy Path"
    elif asset_ok and not billing_ok:
        return "Service No Bill"
    elif not asset_ok and billing_ok:
        return "Bill No Service"
    else:
        return "DI Issue"

merged["KPI"] = merged.apply(classify_kpi, axis=1)

# ---------------------------------------------------------------------
# CONTROL BRANCHING
# ---------------------------------------------------------------------
if control_type == "Completeness":
    st.subheader("🧩 Completeness Summary")

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

    st.subheader("📋 Completeness Report Details")
    st.dataframe(result_df)

    csv = result_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download Full Report (CSV)",
        data=csv,
        file_name=f"{selected_product}_completeness_report.csv",
        mime="text/csv"
    )

elif control_type == "Accuracy":
    # ---------------- ACCURACY LOGIC ----------------
    st.subheader("🎯 Accuracy Summary (on Completeness Happy Path)")
    happy_df = merged[merged["KPI"] == "Happy Path"].copy()

    for col in ["asset_amount", "billing_amount"]:
        if col in happy_df.columns:
            happy_df[col] = pd.to_numeric(happy_df[col], errors="coerce")

    ABS_TOL = 0.01

    def classify_accuracy(row):
        a = row.get("asset_amount")
        b = row.get("billing_amount")
        if pd.isna(a) or pd.isna(b):
            return "Insufficient Data"
        diff = b - a
        if abs(diff) <= ABS_TOL:
            return "Happy Path"
        elif diff > 0:
            return "Over Billing"
        else:
            return "Under Billing"

    happy_df["Accuracy_KPI"] = happy_df.apply(classify_accuracy, axis=1)
    happy_df["diff_amount"] = happy_df["billing_amount"] - happy_df["asset_amount"]

    total = len(happy_df)
    accurate = (happy_df["Accuracy_KPI"] == "Happy Path").sum()
    overb = (happy_df["Accuracy_KPI"] == "Over Billing").sum()
    underb = (happy_df["Accuracy_KPI"] == "Under Billing").sum()
    insuff = (happy_df["Accuracy_KPI"] == "Insufficient Data").sum()

    accuracy_pct = round((accurate / total) * 100, 2) if total > 0 else 0.0

    c1, c2 = st.columns(2)
    with c1:
        st.metric("🧾 Happy Path Records", f"{total:,}")
    with c2:
        st.metric("✅ Accurate (%)", f"{accuracy_pct} %")

    c3, c4, c5, c6 = st.columns(4)
    with c3:
        st.metric("✅ Accurate", f"{accurate:,}")
    with c4:
        st.metric("⬆️ Over Billing", f"{overb:,}")
    with c5:
        st.metric("⬇️ Under Billing", f"{underb:,}")
    with c6:
        st.metric("❓ Insufficient Data", f"{insuff:,}")

    st.subheader("📋 Accuracy Details")
    st.dataframe(happy_df[[
        "siebel_account_id",
        "billing_service_number",
        "siebel_service_number",
        "asset_amount",
        "billing_amount",
        "diff_amount",
        "product_name",
        "Accuracy_KPI"
    ]])

    csv = happy_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download Accuracy Report (CSV)",
        data=csv,
        file_name=f"{selected_product}_accuracy_report.csv",
        mime="text/csv"
    )

# ---------------- FINAL RESTART OPTION ----------------
st.markdown("---")
st.subheader("🔁 Start Over")

if st.button("🏠 Back to Step 1 (Choose Control Type)"):
    reset_session()
    st.rerun()
