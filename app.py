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
st.sidebar.info("Mappings from different systems guide the SQL generation.")

st.write("Loading mapping files from GCS...")
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Telecom Data Query Agent (Gemini 2.5 Flash)")
st.markdown("Enter a natural language query, and Gemini will generate and run SQL in BigQuery.")

prompt = st.text_area("Enter your query:")

if st.button("Run Query"):
    if not prompt.strip():
        st.warning("Please enter a query prompt!")
    else:
        try:
            with st.spinner("🧠 Generating SQL with Gemini 2.5 Flash..."):
                sql_query = vertex_agent.prompt_to_sql(prompt, siebel_mapping, antillia_mapping)
            st.code(sql_query, language="sql")

            with st.spinner("🏃 Executing SQL in BigQuery..."):
                df = bq_agent.execute(sql_query)

            st.success(f"✅ Query executed successfully! {len(df)} rows returned.")
            st.dataframe(df)

            numeric_cols = df.select_dtypes(include="number").columns.tolist()
            if numeric_cols:
                st.subheader("📈 Charts")
                for col in numeric_cols:
                    st.bar_chart(df[col])

        except Exception as e:
            st.error(f"❌ Error: {e}")
