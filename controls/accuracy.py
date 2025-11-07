import streamlit as st
import pandas as pd

def run_accuracy(system_dfs, selected_product):
    """Run accuracy control only for Happy Path from completeness."""
    merged = system_dfs.get("merged_data", pd.DataFrame())

    if merged.empty or "KPI" not in merged.columns:
        st.warning("Run Completeness first before Accuracy.")
        return merged, pd.DataFrame()

    df = merged[merged["KPI"] == "Happy Path"].copy()
    df["accuracy_flag"] = df.apply(lambda r: (
        "Over Billing" if r.get("billing_amount", 0) > r.get("asset_amount", 0)
        else "Under Billing" if r.get("billing_amount", 0) < r.get("asset_amount", 0)
        else "Accurate"
    ), axis=1)

    st.metric("🎯 Accurate", (df["accuracy_flag"] == "Accurate").sum())
    st.metric("📈 Over Billing", (df["accuracy_flag"] == "Over Billing").sum())
    st.metric("📉 Under Billing", (df["accuracy_flag"] == "Under Billing").sum())

    return merged, df
