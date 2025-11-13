import pandas as pd

def run_accuracy(system_dfs, selected_product):
    """
    Accuracy Control:
    - Runs only on Happy Path records from Completeness.
    - Compares billing vs. asset amounts to find Over/Under Billing.
    """

    merged = system_dfs.get("merged_data", pd.DataFrame())

    if merged.empty or "KPI" not in merged.columns:
        raise ValueError("⚠️ Completeness must be executed first.")

    # --- Filter Happy Path records ---
    df = merged[merged["KPI"] == "Happy Path"].copy()
    if df.empty:
        raise ValueError("No Happy Path records found for Accuracy control.")

    # --- Identify amount columns dynamically ---
    billing_cols = [c for c in df.columns if ("billing" in c.lower() and "amount" in c.lower()) or c.lower() == "charge_amount"]
    asset_cols = [c for c in df.columns if ("asset" in c.lower() and "amount" in c.lower()) or c.lower() == "maintenance_cost"]

    billing_col = billing_cols[0] if billing_cols else None
    asset_col = asset_cols[0] if asset_cols else None
    if not billing_col or not asset_col:
        raise ValueError("Missing billing or asset amount columns for comparison.")

    # --- Compute Accuracy KPI ---
    def classify_accuracy(row):
        billing_amount = float(row.get(billing_col, 0) or 0)
        asset_amount = float(row.get(asset_col, 0) or 0)

        if abs(billing_amount - asset_amount) < 0.01:
            return "Accurate"
        elif billing_amount > asset_amount:
            return "Over Billing"
        else:
            return "Under Billing"

    df["accuracy_flag"] = df.apply(classify_accuracy, axis=1)

    # --- Summary ---
    total = len(df)
    accurate = (df["accuracy_flag"] == "Accurate").sum()
    over_billing = (df["accuracy_flag"] == "Over Billing").sum()
    under_billing = (df["accuracy_flag"] == "Under Billing").sum()
    accuracy_pct = round((accurate / total) * 100, 2) if total else 0.0

    summary = pd.DataFrame({
        "Metric": ["Total Records", "Accurate", "Over Billing", "Under Billing", "Accuracy %"],
        "Value": [total, accurate, over_billing, under_billing, accuracy_pct]
    })

    return df, summary
