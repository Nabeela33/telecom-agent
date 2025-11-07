import streamlit as st
import pandas as pd

def is_available(status):
    if pd.isna(status):
        return False
    return str(status).strip().lower() in ["active", "completed", "complete"]

def run_completeness(system_dfs, selected_product):
    billing_products = system_dfs.get("antillia_billing_products", pd.DataFrame())
    billing_accounts = system_dfs.get("antillia_billing_accounts", pd.DataFrame())
    assets = system_dfs.get("siebel_siebel_assets", pd.DataFrame())
    accounts = system_dfs.get("siebel_siebel_accounts", pd.DataFrame())

    merged = billing_products.merge(billing_accounts, on="billing_account_id", how="left") \
                             .merge(assets, on="asset_id", how="left") \
                             .merge(accounts, on="account_id", how="left")

    merged["service_no_bill"] = (
        merged["asset_status"].apply(is_available) &
        ~merged["billing_account_status"].apply(is_available)
    )
    merged["no_service_bill"] = (
        ~merged["asset_status"].apply(is_available) &
        merged["billing_account_status"].apply(is_available)
    )

    def classify(row):
        asset_ok = is_available(row.get("asset_status"))
        billing_ok = is_available(row.get("billing_account_status"))
        if asset_ok and billing_ok:
            return "Happy Path"
        elif asset_ok and not billing_ok:
            return "Service No Bill"
        elif not asset_ok and billing_ok:
            return "Bill No Service"
        else:
            return "DI Issue"

    merged["KPI"] = merged.apply(classify, axis=1)
    result_df = merged[["asset_id", "account_id", "product_name", "asset_status", "billing_account_status", "KPI"]]

    st.metric("✅ Happy Path", (result_df["KPI"] == "Happy Path").sum())
    st.metric("⚠️ Service No Bill", (result_df["KPI"] == "Service No Bill").sum())
    st.metric("🚫 Bill No Service", (result_df["KPI"] == "Bill No Service").sum())

    return merged, result_df
