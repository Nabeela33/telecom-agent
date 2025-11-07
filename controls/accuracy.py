import pandas as pd
import streamlit as st

def run_accuracy(merged, selected_product):
    st.subheader("🎯 Accuracy Check (Only on Completeness Happy Path)")
    happy_df = merged[merged["KPI"] == "Happy Path"].copy()

    for col in ["asset_amount", "billing_amount"]:
        if col in happy_df.columns:
            happy_df[col] = pd.to_numeric(happy_df[col], errors="coerce")

    def classify_accuracy(row):
        a, b = row.get("asset_amount"), row.get("billing_amount")
        if pd.isna(a) or pd.isna(b):
            return "Insufficient Data"
        diff = b - a
        if abs(diff) <= 0.01:
            return "Accurate"
        elif diff > 0:
            return "Over Billing"
        else:
            return "Under Billing"

    happy_df["Accuracy_KPI"] = happy_df.apply(classify_accuracy, axis=1)
    happy_df["diff_amount"] = happy_df["billing_amount"] - happy_df["asset_amount"]

    total = len(happy_df)
    accurate = (happy_df["Accuracy_KPI"] == "Accurate").sum()
    overb = (happy_df["Accuracy_KPI"] == "Over Billing").sum()
    underb = (happy_df["Accuracy_KPI"] == "Under Billing").sum()
    insuff = (happy_df["Accuracy_KPI"] == "Insufficient Data").sum()
    pct = round((accurate / total) * 100, 2) if total else 0.0

    c1, c2 = st.columns(2)
    c1.metric("Records", f"{total:,}")
    c2.metric("Accurate (%)", f"{pct}%")

    st.dataframe(happy_df)
    csv = happy_df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download Accuracy Report", csv, f"{selected_product}_accuracy.csv", "text/csv")

    return happy_df
