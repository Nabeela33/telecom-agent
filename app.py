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
st.sidebar.info("Mappings from different systems guide the SQL generation.")
st.sidebar.caption("Using Gemini 2.5 Flash and BigQuery")

# ---------------- INIT ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# Preload mapping silently
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- MAIN UI ----------------
st.title("📊 Telecom Data Assistant")
st.markdown("Ask your question in plain English — Gemini will understand, generate SQL, and fetch data from BigQuery!")

prompt = st.text_area("💬 Your question:")

if st.button("🚀 Run Query"):
    if not prompt.strip():
        st.warning("Please enter a question or query prompt!")
    else:
        try:
            # Generate SQL
            with st.spinner("🧠 Thinking with Gemini..."):
                sql_query = vertex_agent.prompt_to_sql(prompt, siebel_mapping, antillia_mapping)

            st.subheader("🪄 Generated SQL")
            st.code(sql_query, language="sql")

            # Execute SQL
            with st.spinner("📡 Fetching results from BigQuery..."):
                df = bq_agent.execute(sql_query)

            st.success(f"✅ Query executed successfully! {len(df)} rows returned.")
            st.dataframe(df)

            # Download as CSV
            csv_data = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv_data,
                file_name="query_results.csv",
                mime="text/csv"
            )

            # --- Conversational Follow-up ---
            st.markdown("---")
            st.markdown("🤖 **What would you like to do next?**")
            next_action = st.radio(
                "Choose an action:",
                ["Nothing, thanks", "Filter this data", "Visualize something", "Summarize these results"]
            )

            if next_action == "Filter this data":
                st.info("🔍 Coming soon: Interactive filters based on your dataset.")
            elif next_action == "Visualize something":
                numeric_cols = df.select_dtypes(include="number").columns.tolist()
                if not numeric_cols:
                    st.warning("No numeric columns available for visualization.")
                else:
                    selected_col = st.selectbox("Select a column to visualize:", numeric_cols)
                    st.bar_chart(df[selected_col])
            elif next_action == "Summarize these results":
                with st.spinner("✨ Summarizing data..."):
                    summary_prompt = f"Summarize this dataset in 3 bullet points:\n\n{df.head(20).to_string()}"
                    summary = vertex_agent.prompt_to_sql(summary_prompt, siebel_mapping, antillia_mapping)
                st.write(summary)

        except Exception as e:
            st.error(f"❌ Error: {e}")
