import streamlit as st
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent  # assuming you have this

# Config
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1"
SIEBEL_MAPPING_FILE = "Mapping files/siebel_mapping.txt"
ANTILLIA_MAPPING_FILE = "Mapping files/antillia_mapping.txt"

# Load mappings
siebel_mapping = load_mapping(BUCKET_NAME, SIEBEL_MAPPING_FILE)
antillia_mapping = load_mapping(BUCKET_NAME, ANTILLIA_MAPPING_FILE)

# Initialize Vertex AI agent
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# Streamlit UI
st.title("📊 Telecom Data Query Agent")
prompt = st.text_area("Enter your query:")

if st.button("Run Query"):
    if not prompt.strip():
        st.warning("Please enter a query prompt!")
    else:
        try:
            with st.spinner("Generating SQL..."):
                sql_query = vertex_agent.prompt_to_sql(prompt, siebel_mapping, antillia_mapping)
            st.code(sql_query, language="sql")

            with st.spinner("Executing SQL..."):
                df = bq_agent.execute(sql_query)

            st.success(f"Query executed successfully! {len(df)} rows returned.")
            st.dataframe(df)

        except Exception as e:
            st.error(f"Error: {e}")
