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
BUCKET_NAME = None
CONTROL_MAPPING_FILE = "config/control_mapping.yaml"
SYSTEM_CONNECTIONS_FILE = "config/system_connections.yaml"

# ---------------- INIT ----------------
st.set_page_config(page_title="Data Quality Controls", layout="wide")
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

st.title("🛡️ Data Quality Controls")
st.markdown("Run data quality controls dynamically using configuration-driven logic.")

if "ai_interpretation" not in st.session_state:
    st.session_state["ai_interpretation"] = ""

# ---------------- AI REQUIREMENT INTERPRETER ----------------
st.markdown("---")
st.subheader("🤖 AI Requirement Interpreter (Optional)")

uploaded_file = st.file_uploader(
    "Upload requirement document",
    type=["txt", "md", "csv"]
)

requirement_text = st.text_area(
    "Or paste requirement here",
    height=120
)

if uploaded_file:
    try:
        requirement_text = uploaded_file.read().decode("utf-8")
    except Exception:
        st.error("Unable to read uploaded file. Please upload a UTF-8 text, markdown, or CSV file.")
        requirement_text = ""

if st.button("🧠 Interpret Requirement with Vertex AI"):
    if not requirement_text.strip():
        st.warning("Please upload or paste a requirement.")
    else:
        with st.spinner("Vertex AI analyzing requirement..."):
            try:
                prompt = f"""
                You are a telecom data quality expert.

                Read the requirement below and extract these fields clearly:

                Control Type:
                Source Systems:
                Target Systems:
                Join Keys:
                Filters:
                Threshold:
                Business Summary:

                Requirement:
                {requirement_text}

                Return the answer in a neat business-friendly format.
                """
                response = vertex_agent.model.generate_content(prompt)
                st.session_state["ai_interpretation"] = response.text
            except Exception as e:
                st.error(f"Vertex AI failed: {str(e)}")

if st.session_state["ai_interpretation"]:
    st.success("AI Interpretation")
    st.markdown(st.session_state["ai_interpretation"])

# ---------------- LOAD CONFIG ----------------
config_data = load_yaml_config(BUCKET_NAME, CONTROL_MAPPING_FILE)

def reset_session():
    for key in ["control_type", "selected_product", "confirmed", "ai_interpretation"]:
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

    try:
        product_df = bq_agent.execute(
            f"SELECT DISTINCT product_name FROM `{PROJECT_ID}.gibantillia.billing_products`"
        )
        product_list = sorted(product_df["product_name"].dropna().tolist())
    except Exception as e:
        st.error(f"Failed to load product list: {str(e)}")
        st.stop()

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

# ---------------- STEP 3: Load Config ----------------
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
    combined_mapping += "\n" + load_mapping(None, f"config/{file}")

# ---------------- STEP 5: Confirm ----------------
if "confirmed" not in st.session_state:
    st.subheader("✅ Step 3: Confirm Selection")
    st.markdown(f"You've chosen to run **{control_type}** for product **{selected_product}**.")
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

# ---------------- STEP 6: Fetch Data ----------------
st.success(f"🚀 Running {control_type} for **{selected_product}**...")

try:
    system_dfs = fetch_system_data(PROJECT_ID, systems)
except Exception as e:
    st.error(f"Failed to fetch source system data: {str(e)}")
    st.stop()

# ---------------- STEP 7: Execute Control Logic ----------------
try:
    if control_type == "Completeness":
        merged, result_df = run_completeness(system_dfs, selected_product)
    elif control_type == "Accuracy":
        merged, result_df = run_accuracy(system_dfs, selected_product)
except Exception as e:
    st.error(f"Control execution failed: {str(e)}")
    st.stop()

# ---------------- STEP 8: Display Output ----------------
st.subheader("📊 Results Summary")
st.dataframe(result_df, use_container_width=True)

st.subheader("📋 Detailed Records")
st.dataframe(merged, use_container_width=True)

summary_csv = result_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Download Summary (CSV)",
    data=summary_csv,
    file_name=f"{selected_product}_{control_type.lower()}_summary.csv",
    mime="text/csv"
)

detail_csv = merged.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Download Detailed Records (CSV)",
    data=detail_csv,
    file_name=f"{selected_product}_{control_type.lower()}_details.csv",
    mime="text/csv"
)

st.markdown("---")
if st.button("🏠 Restart"):
    reset_session()
    st.rerun()
