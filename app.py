import streamlit as st
import pandas as pd
from utils import load_mapping
from vertex_client import VertexAgent
from bigquery_client import BigQueryAgent

# ---------------- CONFIG ----------------
PROJECT_ID = "telecom-data-lake"
REGION = "europe-west2"
BUCKET_NAME = "stage_data1/Mapping files"

# ---------------- LOAD MAPPINGS ----------------
st.sidebar.title("Configuration")
st.sidebar.info("Select control type and product for the completeness report.")

siebel_mapping = load_mapping(BUCKET_NAME, "siebel_mapping.txt")
antillia_mapping = load_mapping(BUCKET_NAME, "antillia_mapping.txt")

# ---------------- INIT AGENTS ----------------
vertex_agent = VertexAgent(PROJECT_ID, REGION)
bq_agent = BigQueryAgent(PROJECT_ID)

# ---------------- STREAMLIT UI ----------------
st.title("📊 Telecom Completeness Control Agent")
st.markdown(
    "This agent calculates service completeness KPIs for telecom products. "
    "Select a control type and product, confirm, and results will be displayed."
)

# Sidebar filters
control_type = st.sidebar.selectbox("Select Control Type", ["Product Completeness"])
product_list = ["Fiber Connection 100Mbps", "Mobile Plan 50GB", "Enterprise VPN"]  # Example products
selected_product = st.sidebar.selectbox("Select Product", product_list)

# Confirmation
confirm = st.button(f"Run Completeness Control for '{selected_product}'")

if confirm:
    try:
        with st.spinner("🧠 Running Completeness Control..."):
            # Fetch Siebel tables
            accounts = bq_agent.execute("SELECT * FROM `o_siebel.siebel_accounts`")
            assets = bq_agent.execute("SELECT * FROM `o_siebel.siebel_assets`")
            orders = bq_agent.execute("SELECT * FROM `o_siebel.siebel_orders`")
            
            # Fetch Antillia tables
            billing_accounts = bq_agent.execute("SELECT * FROM `gibantillia.billing_accounts`")
            billing_products = bq_agent.execute("SELECT * FROM `gibantillia.billing_products` WHERE product_name = '{}'".format(selected_product))
            
            # Rename columns to avoid conflicts
            accounts = accounts.rename(columns={"account_id": "siebel_account_id"})
            assets = assets.rename(columns={"account_id": "siebel_account_id", "status": "asset_status"})
            orders = orders.rename(columns={"account_id": "siebel_account_id"})
            billing_accounts = billing_accounts.rename(columns={"account_id": "siebel_account_id", "status": "billing_account_status"})
            
            # Merge tables
            merged = billing_products.merge(
                billing_accounts[['billing_account_id', 'billing_account_status', 'siebel_account_id']],
                on='billing_account_id',
                how='left'
            )
            
            merged = merged.merge(
                assets[['asset_id', 'asset_status', 'siebel_account_id']],
                on='asset_id',
                how='left'
            )
            
            merged = merged.merge(
                orders[['order_id', 'siebel_account_id', 'asset_id']],
                on=['order_id', 'asset_id', 'siebel_account_id'],
                how='left'
            )
            
            # KPIs
            merged["service_no_bill"] = (
                (merged["asset_status"] == "Operational") & 
                (merged["billing_account_status"] != "Active")
            )
            
            merged["no_service_bill"] = (
                (merged["asset_status"] != "Operational") & 
                (merged["billing_account_status"] == "Active")
            )
            
            # Display results
            st.success(f"✅ Completeness Control executed for '{selected_product}'")
            
            with st.expander("View Results"):
                st.dataframe(merged)
                
                # Download CSV
                csv = merged.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"{selected_product}_completeness.csv",
                    mime='text/csv'
                )
                
            # Summary KPIs
            st.subheader("📊 KPIs")
            st.metric("Accounts with Service but No Billing", merged["service_no_bill"].sum())
            st.metric("Accounts with Billing but No Service", merged["no_service_bill"].sum())
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
