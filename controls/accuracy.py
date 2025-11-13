import streamlit as st
import pandas as pd

def run_accuracy(system_dfs, selected_product):
    """
    Accuracy Control:
    Works on 'Happy Path' records from Completeness.
    Compares billing and asset amounts for discrepancies.
    """

    merged = system_dfs.get("merged_data", pd.DataFrame())

    if merged.empty or "KPI" not in merged.columns:
        st.warning("⚠️ Please run Completeness first before executing Accuracy.")
        return merged, pd.DataFrame()

    # --- Filter Happy Path records ---
    df = merged[merged["KPI"] == "Happy Path"].copy()
    if df.empty:
        st.warning("No Happy Path records found for Accuracy check.")
        return merged, pd.DataFrame()

    # --- Identify relevant columns ---
    billing_cols = [c for c in df.columns if ("billing" in c.lower() and "amount" in c.lower()) or c.lower() == "charge_amount"]
    asset_cols = [c for c in df.columns if ("asset" in c.lower() and "amount" in c.lower()) or c.lower() == "maintenance_cost"]

    billing_col = billing_cols[0] if billing_cols else None
    asset_col = asset_cols[0] if asset_cols else None

    if not billing_col or not asset_col:
        st.error("Missing billing or asset amount columns for Accuracy comparison.")
        return merged, pd.DataFrame()

    # --- Compute accuracy KPIs ---
    df["accuracy_flag"] = df.apply(
        lambda r: (
            "Over Billing"
            if float(r.get(billing_col, 0) or 0) > float(r.get(asset_col, 0) or 0)
            else "Under Billing"
            if float(r.get(billing_col, 0) or 0) < float(r.get(asset_col, 0) or 0)
            else "Accurate"
        ),
        axis=1
    )

    total = len(df)
    accurate = (df["accuracy_flag"] == "Accurate").sum()
    over_billing = (df["accuracy_flag"] == "Over Billing").sum()
    under_billing = (df["accuracy_flag"] == "Under Billing").sum()

    st.subheader("🎯 Accuracy Summary")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("📊 Total", f"{total:,}")
    with c2:
        st.metric("✅ HappyPath", f"{accurate:,}")
    with c3:
        st.metric("⚠️ Over Billing", f"{over_billing:,}")
    with c4:
        st.metric("📉 Under Billing", f"{under_billing:,}")

    accuracy_pct = round((accurate / total * 100) if total else 0, 2)
    st.metric("🎯 Accuracy (%)", f"{accuracy_pct} %")

    # --- Show detailed results ---
    st.subheader("📋 Accuracy Details")
    display_cols = [
        "siebel_account_id",
        "asset_id",
        "product_name",
        billing_col,
        asset_col,
        "accuracy_flag",
    ]
    display_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(df[display_cols])

    # --- CSV Download ---
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download Accuracy Report (CSV)",
        data=csv,
        file_name=f"{selected_product}_accuracy_report.csv",
        mime="text/csv",
    )

    return merged, df
