import pandas as pd
import streamlit as st

def is_available(status):
    if pd.isna(status):
        return False
    return str(status).strip().lower() in ["active", "completed", "complete"]

def run_completeness(merged, selected_product):
    merged["service_no_bill"] = (
        merged["asset_status"].apply(is_available)
        & ~merged["billing_account_status"].apply(is_available)
    )
    merged["no_service_bill"] = (
        ~merged["asset_status"].apply(is_available)
        & merged["billing_account_status"].apply(is_available)
    )

    def classify_kpi(row):
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

    merged["KPI"] = merged.apply(classify_kpi, axis=1)
    result_df = merged.drop_duplicates(subset=["asset_id", "product_name"])

    st.subheader("🧩 Completeness Summary")
    total = len(result_df)
    happy = (result_df["KPI"] == "Happy Path").sum()
    service_no_bill = (result_df["KPI"] == "Service No Bill").sum()
    no_service_bill = (result_df["KPI"] == "Bill No Service").sum()
    completeness_pct = round((happy / total) * 100, 2) if total else 0.0

    c1, c2 = st.columns(2)
    c1.metric("Total Records", f"{total:,}")
    c2.metric("Happy Path (%)", f"{completeness_pct}%")

    st.dataframe(result_df)
    csv = result_df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download Completeness Report", csv, f"{selected_product}_completeness.csv", "text/csv")

    return merged, result_df
