import streamlit as st
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent
import pandas as pd
import altair as alt

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
st.title("📊 Telecom Data Query Agent")
st.markdown("Enter your query, and Gemini will generate SQL to fetch data from BigQuery.")

prompt = st.text_area("Enter your query:")

if st.button("Run Query"):
    if not prompt.strip():
        st.warning("Please enter a query prompt!")
    else:
        try:
            # Generate SQL
            with st.spinner("🧠 Generating SQL"):
                sql_query = vertex_agent.prompt_to_sql(prompt, siebel_mapping, antillia_mapping)
            st.subheader("Generated SQL")
            st.code(sql_query, language="sql")

            # Execute SQL
            with st.spinner("🏃 Executing SQL in BigQuery..."):
                df = bq_agent.execute(sql_query)

            st.success(f"✅ Query executed successfully! {len(df)} rows returned.")

            # ---------------- Summary Stats ----------------
            st.subheader("Summary Stats")
            st.metric("Total Rows", len(df))
            numeric_cols = df.select_dtypes(include="number").columns.tolist()
            for col in numeric_cols:
                st.metric(f"{col} Avg", round(df[col].mean(), 2))
                st.metric(f"{col} Max", round(df[col].max(), 2))
                st.metric(f"{col} Min", round(df[col].min(), 2))

            # ---------------- Filtering ----------------
            st.subheader("Filter Data")
            filtered_df = df.copy()
            for col in df.select_dtypes(include="object").columns:
                unique_vals = df[col].unique()
                selected = st.multiselect(f"Filter {col}", options=unique_vals, default=unique_vals)
                filtered_df = filtered_df[filtered_df[col].isin(selected)]

            # Collapsible dataframe
            with st.expander("View Filtered Data"):
                st.dataframe(filtered_df)

            # ---------------- Pivot Table ----------------
            if st.checkbox("Create Pivot Table"):
                st.subheader("Pivot Table")
                pivot_index = st.selectbox("Pivot Index", filtered_df.columns, key="pivot_index")
                pivot_values = st.selectbox("Pivot Values (numeric)", numeric_cols, key="pivot_values")
                agg_func = st.selectbox("Aggregation Function", ["sum", "mean", "max", "min"], key="pivot_agg")
                pivot_table = filtered_df.pivot_table(index=pivot_index, values=pivot_values, aggfunc=agg_func)
                st.dataframe(pivot_table)

            # ---------------- Interactive Charts ----------------
            if numeric_cols:
                st.subheader("Visualize Data")
                col_to_plot = st.selectbox("Select numeric column to visualize", numeric_cols, key="chart_col")
                chart_type = st.selectbox("Select chart type", ["Bar Chart", "Line Chart", "Area Chart"], key="chart_type")

                chart = alt.Chart(filtered_df.reset_index()).encode(
                    x=alt.X('index:O', title='Row'),
                    y=alt.Y(col_to_plot, title=col_to_plot)
                )

                if chart_type == "Bar Chart":
                    chart = chart.mark_bar()
                elif chart_type == "Line Chart":
                    chart = chart.mark_line()
                elif chart_type == "Area Chart":
                    chart = chart.mark_area()

                st.altair_chart(chart.interactive(), use_container_width=True)

            # ---------------- Download CSV ----------------
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Filtered Data as CSV", data=csv, file_name="query_result.csv", mime="text/csv")

        except Exception as e:
            st.error(f"❌ Error: {e}")
