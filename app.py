import streamlit as st
import pandas as pd
from google.cloud import storage
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
from io import BytesIO, StringIO

# ----------------- CONFIG -----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1"
SIEBEL_MAPPING_FILE = "Mapping files/siebel_mapping.txt"   # <-- flexible format
ANTILLIA_MAPPING_FILE = "Mapping files/antillia_mapping.txt"

# ----------------- FUNCTIONS -----------------
def load_mapping(bucket_name, file_name):
    """
    Load mapping file from GCS (.csv, .txt, or .xlsx)
    Returns pandas DataFrame
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_bytes()

    if file_name.endswith(".xlsx"):
        df = pd.read_excel(BytesIO(content))
    elif file_name.endswith(".csv") or file_name.endswith(".txt"):
        try:
            df = pd.read_csv(StringIO(content.decode("utf-8")), sep="\t")
        except Exception:
            df = pd.read_csv(StringIO(content.decode("utf-8")), sep=",")
    else:
        raise ValueError(f"Unsupported mapping file format: {file_name}")
    
    return df

# ----------------- LOAD MAPPINGS -----------------
st.sidebar.title("Configuration")
st.sidebar.info("Mappings from Siebel & Antillia help generate accurate SQL.")

siebel_mapping = load_mapping(BUCKET_NAME, SIEBEL_MAPPING_FILE)
antillia_mapping = load_mapping(BUCKET_NAME, ANTILLIA_MAPPING_FILE)

# ----------------- INIT AGENTS -----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION, model_name="gemini-2.5-flash")
bq_agent = BigQueryAgent(PROJECT_ID)

# ----------------- STREAMLIT UI -----------------
st.title("📊 Telecom Data Query Agent (Gemini 2.5)")
st.markdown(
    "Ask questions in natural language, and Gemini 2.5 will convert them into BigQuery SQL."
)

prompt = st.text_area("Enter your query:")

with st.expander("Preview Mapping Files"):
    st.subheader("Siebel Mapping")
    st.dataframe(siebel_mapping.head())
    st.subheader("Antillia Mapping")
    st.dataframe(antillia_mapping.head())

if st.button("Run Query"):
    if not prompt.strip():
        st.warning("Please enter a query prompt!")
    else:
        try:
            # ----------------- GENERATE SQL -----------------
            with st.spinner("💡 Generating SQL with Gemini 2.5..."):
                sql_query = vertex_agent.prompt_to_sql(prompt, siebel_mapping, antillia_mapping)
            st.code(sql_query, language="sql")

            # ----------------- EXECUTE SQL -----------------
            with st.spinner("🔍 Executing SQL in BigQuery..."):
                df = bq_agent.execute(sql_query)
            
            st.success(f"✅ Query executed successfully — {len(df)} rows returned.")
            st.dataframe(df)

            # ----------------- OPTIONAL CHARTS -----------------
            numeric_cols = df.select_dtypes(include="number").columns.tolist()
            if numeric_cols:
                st.subheader("📈 Quick Charts")
                for col in numeric_cols:
                    st.bar_chart(df[col])

        except Exception as e:
            st.error(f"❌ Error: {e}")
