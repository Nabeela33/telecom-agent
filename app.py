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
st.markdown("Hello!! Please enter your query. Gemini will generate SQL and fetch data from BigQuery.")

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

            # Collapsible dataframe
            with st.expander("View Query Results"):
                st.dataframe(df)

            # ---------------- Visualization Popup ----------------
            if not df.empty:
                viz_request = st.button("Visualize Results")
                if viz_request:
                    with st.form("visualization_form"):
                        st.markdown("### Select visualization options")
                        numeric_cols = df.select_dtypes(include="number").columns.tolist()
                        if not numeric_cols:
                            st.info("No numeric columns available for visualization.")
                        else:
                            col_to_plot = st.selectbox("Select column to visualize:", numeric_cols)
                            chart_type = st.selectbox("Select chart type:", ["Bar", "Line", "Scatter"])
                            submit_viz = st.form_submit_button("Generate Chart")

                            if submit_viz:
                                chart = None
                                if chart_type == "Bar":
                                    chart = alt.Chart(df).mark_bar().encode(
                                        x=alt.X(df.index, title="Row"),
                                        y=alt.Y(col_to_plot, title=col_to_plot)
                                    )
                                elif chart_type == "Line":
                                    chart = alt.Chart(df).mark_line(point=True).encode(
                                        x=alt.X(df.index, title="Row"),
                                        y=alt.Y(col_to_plot, title=col_to_plot)
                                    )
                                elif chart_type == "Scatter":
                                    chart = alt.Chart(df).mark_circle(size=60).encode(
                                        x=alt.X(df.index, title="Row"),
                                        y=alt.Y(col_to_plot, title=col_to_plot)
                                    )

                                st.altair_chart(chart.interactive(), use_container_width=True)

        except Exception as e:
            st.error(f"❌ Error: {e}")
