import json
import difflib
import pandas as pd
import streamlit as st

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
st.markdown("Run data quality controls using AI-interpreted requirements.")

# ---------------- LOAD CONFIG ----------------
config_data = load_yaml_config(BUCKET_NAME, CONTROL_MAPPING_FILE)


def reset_session():
    for key in [
        "requirement_text",
        "ai_interpretation",
        "control_type",
        "selected_product",
        "confirmed"
    ]:
        if key in st.session_state:
            del st.session_state[key]


def normalize_control_type(value: str) -> str:
    if not value:
        return ""
    value = value.strip().lower()
    if "complete" in value:
        return "Completeness"
    if "accur" in value:
        return "Accuracy"
    return value.title()


def read_uploaded_requirement(uploaded_file) -> str:
    if uploaded_file is None:
        return ""

    file_name = uploaded_file.name.lower()

    try:
        if file_name.endswith(".txt") or file_name.endswith(".md"):
            return uploaded_file.read().decode("utf-8")

        if file_name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
            return df.to_csv(index=False)

        if file_name.endswith(".xlsx"):
            sheets = pd.read_excel(uploaded_file, sheet_name=None)
            parts = []
            for sheet_name, df in sheets.items():
                parts.append(f"\n--- SHEET: {sheet_name} ---\n")
                parts.append(df.fillna("").to_csv(index=False))
            return "\n".join(parts)

        return uploaded_file.read().decode("utf-8")

    except Exception as e:
        raise ValueError(f"Unable to read uploaded file: {e}")


def load_product_list() -> list:
    product_df = bq_agent.execute(
        f"SELECT DISTINCT product_name FROM `{PROJECT_ID}.gibantillia.billing_products`"
    )
    return sorted(product_df["product_name"].dropna().astype(str).tolist())


def resolve_product_name(ai_product: str, product_list: list) -> str:
    if not ai_product:
        return ""

    ai_product_clean = ai_product.strip()
    ai_product_lower = ai_product_clean.lower()

    # Exact case-insensitive match
    for product in product_list:
        if product.lower() == ai_product_lower:
            return product

    # Containment match
    for product in product_list:
        if ai_product_lower in product.lower() or product.lower() in ai_product_lower:
            return product

    # Fuzzy match
    matches = difflib.get_close_matches(ai_product_clean, product_list, n=1, cutoff=0.6)
    return matches[0] if matches else ""


def interpret_requirement(requirement_text: str, product_list: list) -> dict:
    prompt = f"""
You are a telecom data quality expert.

Read the requirement below and extract the following fields.
Return ONLY valid JSON. Do not return markdown. Do not wrap in code fences.

Required JSON format:
{{
  "control_type": "Completeness or Accuracy",
  "product_name": "exact or closest product mentioned in requirement",
  "source_systems": ["..."],
  "target_systems": ["..."],
  "join_keys": ["..."],
  "filters": ["..."],
  "threshold": "string or number",
  "business_summary": "short summary"
}}

Valid control types are:
- Completeness
- Accuracy

Known product names:
{product_list[:200]}

Requirement:
{requirement_text}
"""

    response = vertex_agent.model.generate_content(prompt)
    raw_text = response.text.strip()

    # Try plain JSON first
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        # Recover JSON if model adds extra text
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("Vertex AI did not return valid JSON.")
        parsed = json.loads(raw_text[start:end + 1])

    parsed["control_type"] = normalize_control_type(parsed.get("control_type", ""))
    parsed["product_name"] = resolve_product_name(parsed.get("product_name", ""), product_list)

    return parsed


# ---------------- SESSION DEFAULTS ----------------
if "ai_interpretation" not in st.session_state:
    st.session_state["ai_interpretation"] = None

# ---------------- REQUIREMENT INPUT ----------------
st.markdown("---")
st.subheader("🤖 AI Requirement Interpreter")

uploaded_file = st.file_uploader(
    "Upload requirement document",
    type=["txt", "md", "csv", "xlsx"]
)

pasted_text = st.text_area(
    "Or paste requirement here",
    height=180,
    placeholder=(
        "Example:\n"
        "Build a completeness control for Broadband Basic.\n"
        "Compare active services between Siebel and Antillia.\n"
        "Records active in service must exist in billing.\n"
        "Join on account_id and asset_id.\n"
        "Threshold 98%."
    )
)

col1, col2 = st.columns([1, 1])

with col1:
    if st.button("🧠 Interpret Requirement with Vertex AI"):
        try:
            uploaded_text = read_uploaded_requirement(uploaded_file) if uploaded_file else ""
            requirement_text = uploaded_text.strip() if uploaded_text.strip() else pasted_text.strip()

            if not requirement_text:
                st.warning("Please upload or paste a requirement.")
                st.stop()

            product_list = load_product_list()
            interpretation = interpret_requirement(requirement_text, product_list)

            if interpretation.get("control_type") not in ["Completeness", "Accuracy"]:
                raise ValueError(
                    f"Unsupported control type returned by AI: {interpretation.get('control_type')}"
                )

            if not interpretation.get("product_name"):
                raise ValueError(
                    "AI could not confidently match the product name with the available product list."
                )

            st.session_state["requirement_text"] = requirement_text
            st.session_state["ai_interpretation"] = interpretation
            st.session_state["control_type"] = interpretation["control_type"]
            st.session_state["selected_product"] = interpretation["product_name"]

            if "confirmed" in st.session_state:
                del st.session_state["confirmed"]

            st.rerun()

        except Exception as e:
            st.error(f"Vertex AI interpretation failed: {str(e)}")

with col2:
    if st.button("🔁 Reset"):
        reset_session()
        st.rerun()

# ---------------- WAIT FOR AI INTERPRETATION ----------------
if not st.session_state.get("ai_interpretation"):
    st.info("Upload or paste a requirement, then click 'Interpret Requirement with Vertex AI' to continue.")
    st.stop()

# ---------------- SHOW AI OUTPUT ----------------
ai_output = st.session_state["ai_interpretation"]
control_type = st.session_state["control_type"]
selected_product = st.session_state["selected_product"]

st.success("AI interpretation completed")

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"**Control Type:** {control_type}")
    st.markdown(f"**Product:** {selected_product}")
    st.markdown(f"**Threshold:** {ai_output.get('threshold', 'N/A')}")

with col2:
    source_systems = ai_output.get("source_systems", [])
    target_systems = ai_output.get("target_systems", [])
    st.markdown(f"**Source Systems:** {', '.join(source_systems) if source_systems else 'N/A'}")
    st.markdown(f"**Target Systems:** {', '.join(target_systems) if target_systems else 'N/A'}")

with st.expander("View AI-extracted details", expanded=True):
    st.json(ai_output)

# ---------------- LOAD CONFIG ----------------
try:
    control_config = get_control_config(control_type, selected_product, config_data)
except Exception as e:
    st.error(f"Unable to load configuration: {str(e)}")
    st.stop()

if not control_config:
    st.error(f"No configuration found for {control_type} → {selected_product}")
    st.stop()

systems = control_config.get("systems", [])
mapping_files = control_config.get("mappings", [])

st.info(f"📚 Systems in scope: {', '.join(systems)}")
st.caption(f"Mapping files: {', '.join(mapping_files)}")

# ---------------- LOAD MAPPINGS ----------------
combined_mapping = ""
try:
    for file in mapping_files:
        combined_mapping += "\n" + load_mapping(None, f"config/{file}")
except Exception as e:
    st.error(f"Failed to load mapping files: {str(e)}")
    st.stop()

# ---------------- CONFIRM ----------------
if "confirmed" not in st.session_state:
    st.subheader("✅ Confirm and Run")
    st.markdown(
        f"AI selected **{control_type}** for product **{selected_product}**."
    )

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

# ---------------- FETCH DATA ----------------
st.success(f"🚀 Running {control_type} for **{selected_product}**...")

try:
    system_dfs = fetch_system_data(PROJECT_ID, systems)
except Exception as e:
    st.error(f"Failed to fetch source system data: {str(e)}")
    st.stop()

# ---------------- EXECUTE CONTROL ----------------
try:
    if control_type == "Completeness":
        merged, result_df = run_completeness(system_dfs, selected_product)
    elif control_type == "Accuracy":
        merged, result_df = run_accuracy(system_dfs, selected_product)
    else:
        raise ValueError(f"Unsupported control type: {control_type}")
except Exception as e:
    st.error(f"Control execution failed: {str(e)}")
    st.stop()

# ---------------- DISPLAY OUTPUT ----------------
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
