import streamlit as st
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
import pandas as pd

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- PAGE SETUP ----------------
st.set_page_config(page_title="Data Quality Controls Assistant", layout="wide")
st.title("🤖 Data Quality Controls Assistant")

# ---------------- LOAD MAPPINGS ----------------
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- SESSION STATE ----------------
if "messages" not in st.session_state:
    st.session_state.messages = []
if "stage" not in st.session_state:
    st.session_state.stage = "ask_control"
if "control_type" not in st.session_state:
    st.session_state.control_type = None
if "product" not in st.session_state:
    st.session_state.product = None
if "confirmed" not in st.session_state:
    st.session_state.confirmed = False
if "show_exceptions" not in st.session_state:
    st.session_state.show_exceptions = False

# ---------------- CHAT DISPLAY ----------------
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# ---------------- STAGE HANDLERS ----------------

def add_assistant_message(text):
    st.session_state.messages.append({"role": "assistant", "content": text})

def add_user_message(text):
    st.session_state.messages.append({"role": "user", "content": text})

def reset_chat():
    st.session_state.clear()
    st.rerun()

# ---------------- CONTROL LOGIC ----------------

if st.session_state.stage == "ask_control":
    if len(st.session_state.messages) == 0:
        add_assistant_message("👋 Hi there! I’m your Data Quality Assistant.\n\nWhich control type would you like to run today?")
        st.rerun()

    user_input = st.chat_input("Type your control type (e.g., Completeness)")
    if user_input:
        add_user_message(user_input)
        st.session_state.control_type = user_input.strip().title()
        add_assistant_message(f"Great choice! You’ve selected **{st.session_state.control_type}** control. Let’s pick a product next.")
        st.session_state.stage = "ask_product"
        st.rerun()

elif st.session_state.stage == "ask_product":
    product_df = bq_agent.execute("SELECT DISTINCT product_name FROM `telecom-data-lake.gibantillia.billing_products`")
    product_list = sorted(product_df['product_name'].dropna().tolist())

    if not any("Please choose one of the following products" in m["content"] for m in st.session_state.messages):
        add_assistant_message("📦 Please choose one of the following products:")
        add_assistant_message(", ".join(product_list[:10]) + " ...")
        st.rerun()

    user_input = st.chat_input("Type product name from the list above")
    if user_input:
        add_user_message(user_input)
        product = user_input.strip()
        if product not in product_list:
            add_assistant_message("❌ That product wasn’t found. Please type an exact name from the list above.")
        else:
            st.session_state.product = product
            add_assistant_message(f"✅ Got it! You’ve selected **{product}**.")
            add_assistant_message(f"Would you like me to proceed with the **{st.session_state.control_type}** control for **{product}**? (yes/no)")
            st.session_state.stage = "confirm"
        st.rerun()

elif st.session_state.stage == "confirm":
    user_input = st.chat_input("Type 'yes' to confirm or 'no' to restart")
    if user_input:
        add_user_message(user_input)
        if user_input.lower() == "yes":
            add_assistant_message("🚀 Perfect! Let’s run the report...")
            st.session_state.confirmed = True
            st.session_state.stage = "run_report"
        else:
            add_assistant_message("🔁 No problem, restarting setup...")
            reset_chat()
        st.rerun()

elif st.session_state.stage == "run_report":
    add_assistant_message(f"Running **{st.session_state.control_type}** control for **{st.session_state.product}**... please wait ⏳")

    selected_product = st.session_state.product

    # -------- Fetch Data --------
    accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_accounts`")
    assets = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_assets`")
    orders = bq_agent.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_orders`")
    billing_accounts = bq_agent.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_accounts`")
    billing_products = bq_agent.execute(f"""
        SELECT * FROM `telecom-data-lake.gibantillia.billing_products`
        WHERE product_name = '{selected_product}'
    """)

    # -------- Rename + Clean --------
    if 'account_id' in accounts.columns:
        accounts = accounts.rename(columns={"account_id": "siebel_account_id"})
    if 'account_id' in assets.columns:
        assets = assets.rename(columns={"account_id": "siebel_asset_account_id"})
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

    # -------- Merge Logic --------
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
    for col in ["billing_service_number", "siebel_service_number"]:
        if col in merged.columns:
            merged[col] = merged[col].astype(str).str.replace(",", "", regex=False)

    # -------- KPIs --------
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

    # -------- Summary --------
    total = len(result_df)
    happy = (result_df["KPI"] == "Happy Path").sum()
    s_nb = (result_df["KPI"] == "Service No Bill").sum()
    n_sb = (result_df["KPI"] == "Bill No Service").sum()
    completeness_pct = round((happy / total) * 100, 2) if total > 0 else 0.0

    add_assistant_message(f"""
📊 **Completeness Summary**

- 🧾 Total Records: {total:,}
- ✅ Happy Path: {happy:,}
- ⚠️ Service No Bill: {s_nb:,}
- 🚫 Bill No Service: {n_sb:,}
- 📈 Completeness: {completeness_pct}%
    """)
    st.dataframe(result_df)

    csv = result_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download Full Report (CSV)",
        data=csv,
        file_name=f"{selected_product}_completeness_report.csv",
        mime="text/csv"
    )

    add_assistant_message("Would you like to see the **top 10 accounts with exceptions**? (yes/no)")
    st.session_state.stage = "ask_exceptions"
    st.session_state.result_df = result_df
    st.rerun()

elif st.session_state.stage == "ask_exceptions":
    user_input = st.chat_input("Type 'yes' or 'no'")
    if user_input:
        add_user_message(user_input)
        if user_input.lower() == "yes":
            st.session_state.show_exceptions = True
            st.session_state.stage = "show_exceptions"
        else:
            add_assistant_message("👍 No problem. You can restart anytime.")
        st.rerun()

elif st.session_state.stage == "show_exceptions":
    result_df = st.session_state.result_df
    issue_type = st.radio("Select issue type", ["Service No Bill", "Bill No Service"], horizontal=True)
    filtered = result_df[result_df["KPI"] == issue_type]

    if len(filtered) == 0:
        add_assistant_message(f"No records found for **{issue_type}** issues.")
    else:
        st.markdown(f"### 📋 Top 10 {issue_type} Records")
        st.dataframe(filtered.head(10))

        issue_csv = filtered.to_csv(index=False).encode("utf-8")
        st.download_button(
            f"⬇️ Download {issue_type} Records",
            data=issue_csv,
            file_name=f"{st.session_state.product}_{issue_type.replace(' ', '_').lower()}_records.csv",
            mime="text/csv"
        )

    st.stop()
