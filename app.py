import streamlit as st
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
import pandas as pd
import re

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
if "table_name" not in st.session_state:
    st.session_state.table_name = None

# ---------------- MAIN UI ----------------
st.title("📊 Telecom Data Assistant")
st.markdown("Hello!!! Ask me about your data — I’ll understand your question, write SQL, and show the results!")

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

            # Extract table name for future use
            match = re.search(r'`([\w\-]+\.[\w\-]+\.[\w\-]+)`', sql_query)
            if match:
                st.session_state.table_name = match.group(1)
                st.info(f"📂 Target Table: `{st.session_state.table_name}`")

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
if st.session_state.sql_query:
    st.markdown("---")
    st.markdown("🤖 **What would you like to do next?**")

    next_action = st.radio(
        "Choose an action:",
        ["Nothing, thanks", "Filter this data", "Visualize something", "Summarize these results"],
        key="next_action"
    )

    # Get the table name from session
    table_name = st.session_state.table_name

    if next_action in ["Filter this data", "Visualize something"]:
        if not table_name:
            st.warning("I couldn’t detect which table was used in the last query.")
        else:
            with st.spinner(f"🔍 Fetching data from `{table_name}`..."):
                df_full = bq_agent.execute(f"SELECT * FROM `{table_name}` LIMIT 1000")

            if next_action == "Filter this data":
                st.subheader(f"🔎 Filter Data from `{table_name}`")

                columns = df_full.columns.tolist()
                selected_col = st.selectbox("Select a column to filter:", columns)
                unique_vals = df_full[selected_col].dropna().unique().tolist()

                if len(unique_vals) > 100:
                    st.info("Too many unique values. Showing first 100.")
                    unique_vals = unique_vals[:100]

                selected_val = st.selectbox("Select a value:", unique_vals)
                filtered_df = df_full[df_full[selected_col] == selected_val]
                st.dataframe(filtered_df)

                csv_data = filtered_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="📥 Download Filtered Data",
                    data=csv_data,
                    file_name="filtered_results.csv",
                    mime="text/csv"
                )

            elif next_action == "Visualize something":
                st.subheader(f"📊 Visualize Data from `{table_name}`")
                numeric_cols = df_full.select_dtypes(include="number").columns.tolist()

                if not numeric_cols:
                    st.warning("No numeric columns found for visualization.")
                else:
                    selected_col = st.selectbox("Select a numeric column to visualize:", numeric_cols)
                    st.bar_chart(df_full[selected_col])

    elif next_action == "Summarize these results":
        st.subheader("🧠 Summary from Gemini")
        with st.spinner("✨ Generating summary..."):
            summary_prompt = f"Summarize key insights from this dataset:\n\n{st.session_state.df.head(20).to_string()}"
            summary = vertex_agent.prompt_to_sql(summary_prompt, siebel_mapping, antillia_mapping)
        st.write(summary)
