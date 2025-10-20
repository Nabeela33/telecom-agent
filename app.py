import streamlit as st
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
import pandas as pd

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- SIDEBAR ----------------
st.sidebar.title("⚙️ Configuration")
st.sidebar.info("Mappings guide SQL generation across systems.")
st.sidebar.caption("Powered by Gemini 2.5 Flash + BigQuery")

# ---------------- INIT ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# Preload mappings silently
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- STREAMLIT STATE ----------------
if "df" not in st.session_state:
    st.session_state.df = None
if "sql_query" not in st.session_state:
    st.session_state.sql_query = None

# ---------------- MAIN UI ----------------
st.title("📊 Telecom Data Assistant")
st.markdown("Ask me about your data — I’ll understand your question, write SQL, and show the results!")

prompt = st.text_area("💬 Your question:")

if st.button("🚀 Run Query"):
    if not prompt.strip():
        st.warning("Please enter a question or query prompt!")
    else:
        try:
            with st.spinner("🧠 Generating SQL using Gemini..."):
                sql_query = vertex_agent.prompt_to_sql(prompt, siebel_mapping, antillia_mapping)
                st.session_state.sql_query = sql_query

            st.subheader("🪄 Generated SQL")
            st.code(sql_query, language="sql")

            with st.spinner("📡 Running SQL in BigQuery..."):
                df = bq_agent.execute(sql_query)
                st.session_state.df = df

            st.success(f"✅ Query executed successfully! {len(df)} rows returned.")
            st.dataframe(st.session_state.df)

            # Download CSV
            csv_data = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv_data,
                file_name="query_results.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"❌ Error: {e}")

# ---------------- CONVERSATIONAL FOLLOW-UP ----------------
if st.session_state.df is not None:
    st.markdown("---")
    st.markdown("🤖 **What would you like to do next?**")

    next_action = st.radio(
        "Choose an action:",
        ["Nothing, thanks", "Filter this data", "Visualize something", "Summarize these results"],
        key="next_action"
    )

    df = st.session_state.df

    if next_action == "Filter this data":
        st.subheader("🔍 Filter Your Data")

        columns = df.columns.tolist()
        selected_col = st.selectbox("Select a column to filter:", columns)
        unique_vals = df[selected_col].dropna().unique().tolist()

        if len(unique_vals) > 100:
            st.info("Too many unique values. Showing first 100.")
            unique_vals = unique_vals[:100]

        selected_val = st.selectbox("Select a value:", unique_vals)
        filtered_df = df[df[selected_col] == selected_val]
        st.dataframe(filtered_df)

        csv_data = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download Filtered Data",
            data=csv_data,
            file_name="filtered_results.csv",
            mime="text/csv"
        )

    elif next_action == "Visualize something":
        st.subheader("📈 Visualize Your Data")
        numeric_cols = df.select_dtypes(include="number").columns.tolist()

        if not numeric_cols:
            st.warning("No numeric columns found for visualization.")
        else:
            selected_col = st.selectbox("Select a column to visualize:", numeric_cols)
            st.bar_chart(df[selected_col])

    elif next_action == "Summarize these results":
        st.subheader("🧠 Summary from Gemini")
        with st.spinner("✨ Generating summary..."):
            summary_prompt = f"Summarize the key insights from this dataset:\n\n{df.head(20).to_string()}"
            summary = vertex_agent.prompt_to_sql(summary_prompt, siebel_mapping, antillia_mapping)
        st.write(summary)
