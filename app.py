import streamlit as st
import pandas as pd
from utils import load_mapping, load_yaml_config, get_control_config
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
from systems.data_loader import fetch_system_data
from controls.completeness import run_completeness
from controls.accuracy import run_accuracy

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"
CONTROL_MAPPING_FILE = "config/control_mapping.yaml"
SYSTEM_CONNECTIONS_FILE = "config/system_connections.yaml"

# ---------------- INIT ----------------
st.set_page_config(page_title="Data Quality Controls", layout="wide")
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

st.title("🛡️ Data Quality Controls Assistant")
st.markdown("Run data quality controls dynamically using configuration-driven logic.")

# ---------------- LOAD CONFIG ----------------
config_data = load_yaml_config(BUCKET_NAME, CONTROL_MAPPING_FILE)

def reset_session():
    """Clears all step-related session state keys to restart the workflow."""
    for key in ["control_type", "selected_product", "confirmed"]:
        if key in st.session_state:
            del st.session_state[key]

# ---------------- STEP 1: Select Control Type ----------------
if "control_type" not in st.session_state:
    st.subheader("🧩 Step 1: Choose Control Type")
    control_type = st.selectbox("Which data quality control would you like to run?", ["Completeness", "Accuracy"])
    if st.button("Next ➡️"):
        st.session_state["control_type"] = control_type
        st.rerun()
    st.stop()

# ---------------- STEP 2: Select Product (Dynamic from BigQuery) ----------------
if "selected_product" not in st.session_state:
    st.subheader("🛠️ Step 2: Select Product")

    # Fetch product list dynamically from BigQuery
    product_df = bq_agent.execute(
        "SELECT DISTINCT product_name FROM `telecom-data-lake.gibantillia.billing_products`"
    )
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

# ---------------- STEP 3: Load Config for Selected Control ----------------
control_type = st.session_state["control_type"]
selected_product = st.session_state["selected_product"]

control_config = get_control_config(control_type, selected_product, config_data)
if not control_config:
    st.error(f"No configuration found for {control_type} → {selected_product}")
    st.stop()

systems = control_config.get("systems", [])
mapping_files = control_config.get("mappings", [])

st.info(f"📚 Systems in scope: {', '.join(systems)}")
st.caption(f"Mapping files: {', '.join(mapping_files)}")

# ---------------- STEP 4: Load Mappings ----------------
combined_mapping = ""
for file in mapping_files:
    combined_mapping += "\n" + load_mapping(BUCKET_NAME, file)

# ---------------- STEP 5: Confirm Selection ----------------
if "confirmed" not in st.session_state:
    st.subheader("✅ Step 3: Confirm Selection")
    st.markdown(f"You've chosen to run **{control_type}** control for product **{selected_product}**.")
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

# ---------------- STEP 6: Fetch System Data Dynamically ----------------
st.success(f"🚀 Running {control_type} control for **{selected_product}**...")
system_dfs = fetch_system_data(PROJECT_ID, BUCKET_NAME, SYSTEM_CONNECTIONS_FILE, systems)

# Combine data from available systems
billing_products = system_dfs.get("antillia_billing_products", pd.DataFrame())
billing_accounts = system_dfs.get("antillia_billing_accounts", pd.DataFrame())
accounts = system_dfs.get("siebel_siebel_accounts", pd.DataFrame())
assets = system_dfs.get("siebel_siebel_assets", pd.DataFrame())
orders = system_dfs.get("siebel_siebel_orders", pd.DataFrame())

merged = billing_products.copy()
for df in [billing_accounts, accounts, assets, orders]:
    if not df.empty:
        merged = pd.merge(merged, df, how="left", left_index=True, right_index=True)

merged = merged.loc[:, ~merged.columns.duplicated()]

# ---------------- STEP 7: Run Selected Control ----------------
if control_type == "Completeness":
    merged, result_df = run_completeness(merged, selected_product)
elif control_type == "Accuracy":
    run_accuracy(merged, selected_product)

# ---------------- FINAL: Restart Option ----------------
st.markdown("---")
st.subheader("🔁 Restart")
if st.button("🏠 Back to Step 1"):
    reset_session()
    st.rerun()
