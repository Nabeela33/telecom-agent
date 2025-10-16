# app.py
import streamlit as st
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent

# ----------------- CONFIG -----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1"
SIEBEL_MAPPING_FILE = "Mapping files/siebel_mapping.txt"
ANTILLIA_MAPPING_FILE = "Mapping files/antillia_mapping.txt"

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
st.markdown("Ask questions in plain English — Gemini will translate them into BigQuery SQL.")

prompt = st.text_area("Enter your query:")

with st.expander("Preview Mapping Files"):
    st.subheader("Siebel Mapping")
    st.dataframe(siebel_mapping.head())
    st.subheader("Antillia Mapping")
    st.dataframe(antillia_mapping.head())

if st.button("Run Query"):
    if not prompt.strip():
        st.warning("Please enter a query first!")
    else:
        try:
            with st.spinner("💡 Generating SQL with Gemini 2.5..."):
                sql_query = vertex_agent.prompt_to_sql(prompt, siebel_mapping, antillia_mapping)
            st.code(sql_query, language="sql")

            with st.spinner("🔍 Running SQL in BigQuery..."):
                df = bq_agent.execute(sql_query)
            
            st.success(f"✅ Query executed successfully! {len(df)} rows returned.")
            st.dataframe(df)

            # Optional: Chart numeric columns
            numeric_cols = df.select_dtypes(include="number").columns.tolist()
            if numeric_cols:
                st.subheader("📈 Quick Charts")
                for col in numeric_cols:
                    st.bar_chart(df[col])

        except Exception as e:
            st.error(f"❌ Error: {e}")
