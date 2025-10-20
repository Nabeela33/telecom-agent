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
st.sidebar.info("Mappings guide SQL generation for Gemini queries.")

st.write("Loading mapping files from GCS...")
siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Telecom Data Query Agent")
st.markdown(
    "You can either:\n"
    "1️⃣ Enter a natural language query and let Gemini generate SQL.\n"
    "2️⃣ Explore tables interactively using filters below."
)

# ---------------- AI-GENERATED QUERY ----------------
st.subheader("🤖 Gemini SQL Query")
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
            with st.expander("View Query Results"):
                st.dataframe(df)

        except Exception as e:
            st.error(f"❌ Error: {e}")

# ---------------- INTERACTIVE EXPLORATION ----------------
st.subheader("🔍 Explore Tables Interactively")

# Step 1: Table selection
tables = bq_agent.list_tables()
selected_table = st.selectbox("Select Table", tables)

if selected_table:
    # Step 2: Column selection
    columns = bq_agent.list_columns(selected_table)
    selected_column = st.selectbox("Select Column", columns)
    
    if selected_column:
        # Step 3: Sample values filter
        sample_values = bq_agent.get_sample_values(selected_table, selected_column)
        selected_values = st.multiselect("Filter values (optional)", sample_values)

        # Step 4: Fetch filtered data
        if selected_values:
            value_list = ", ".join([repr(v) for v in selected_values])
            filter_query = f"SELECT * FROM `{PROJECT_ID}.{selected_table}` WHERE {selected_column} IN ({value_list}) LIMIT 100"
        else:
            filter_query = f"SELECT * FROM `{PROJECT_ID}.{selected_table}` LIMIT 100"

        with st.spinner("Fetching data..."):
            df_filtered = bq_agent.execute(filter_query)

        with st.expander("View Filtered Data"):
            st.dataframe(df_filtered)

        # Step 5: Optional chart
        if not df_filtered.empty:
            numeric_cols = df_filtered.select_dtypes(include="number").columns.tolist()
            if numeric_cols:
                st.subheader("📈 Visualize Numeric Column")
                col_to_plot = st.selectbox("Select numeric column to chart", numeric_cols)
                chart = alt.Chart(df_filtered).mark_bar().encode(
                    x=alt.X(df_filtered.index, title="Row"),
                    y=alt.Y(col_to_plot, title=col_to_plot)
                ).interactive()
                st.altair_chart(chart, use_container_width=True)
