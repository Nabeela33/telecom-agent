import pandas as pd

def run_completeness(system_dfs, selected_product):
    """
    Completeness Control:
    - Validates data consistency between Siebel and Antillia systems.
    - Computes Happy Path, Service No Bill, and Bill No Service KPIs.
    - Saves merged output in system_dfs["merged_data"] for downstream Accuracy control.
    """

    # --- Extract datasets from system_dfs ---
    accounts = system_dfs.get("siebel_accounts")
    assets = system_dfs.get("siebel_assets")
    orders = system_dfs.get("siebel_orders")
    billing_accounts = system_dfs.get("billing_accounts")
    billing_products = system_dfs.get("billing_products")

    for name, df in {
        "siebel_accounts": accounts,
        "siebel_assets": assets,
        "siebel_orders": orders,
        "billing_accounts": billing_accounts,
        "billing_products": billing_products,
    }.items():
        if df is None:
            raise ValueError(f"❌ Missing dataset: {name}")

    # --- Safe renaming for consistent join keys ---
    if "account_id" in accounts.columns:
        accounts = accounts.rename(columns={"account_id": "siebel_account_id"})
    if "account_id" in assets.columns:
        assets = assets.rename(columns={"account_id": "siebel_asset_account_id"})
    if "account_id" in orders.columns:
        orders = orders.rename(columns={"account_id": "siebel_order_account_id"})
    if "account_id" in billing_accounts.columns:
        billing_accounts = billing_accounts.rename(columns={"account_id": "billing_account_siebel_account_id"})
    if "billing_account_id" in billing_products.columns:
        billing_products = billing_products.rename(columns={"billing_account_id": "billing_account_id_bp"})
    if "billing_account_id" in billing_accounts.columns:
        billing_accounts = billing_accounts.rename(columns={"billing_account_id": "billing_account_id_bacc"})

    # --- Merge logic ---
    merged = (
        billing_products.merge(
            billing_accounts, left_on="billing_account_id_bp", right_on="billing_account_id_bacc", how="left"
        )
        .merge(
            accounts, left_on="billing_account_siebel_account_id", right_on="siebel_account_id", how="left"
        )
        .merge(
            assets, on="asset_id", how="left"
        )
        .merge(
            orders,
            left_on=["asset_id", "siebel_account_id"],
            right_on=["asset_id", "siebel_order_account_id"],
            how="left",
            suffixes=("", "_order")
        )
    )
    merged = merged.loc[:, ~merged.columns.duplicated()]

    # --- Availability logic ---
    def is_available(status):
        if pd.isna(status):
            return False
        return str(status).strip().lower() in ["active", "completed", "complete"]

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

    result_df = merged[[
        "billing_service_number",
        "siebel_service_number",
        "siebel_account_id",
        "asset_id",
        "product_name",
        "asset_status",
        "billing_account_status",
        "KPI"
    ]].drop_duplicates()

    # --- Add summary KPIs ---
    total = len(result_df)
    happy_path = (result_df["KPI"] == "Happy Path").sum()
    service_no_bill = (result_df["KPI"] == "Service No Bill").sum()
    no_service_bill = (result_df["KPI"] == "Bill No Service").sum()
    completeness_pct = round((happy_path / total) * 100, 2) if total else 0.0

    summary = pd.DataFrame({
        "Metric": ["Total Records", "Happy Path", "Service No Bill", "Bill No Service", "Completeness %"],
        "Value": [total, happy_path, service_no_bill, no_service_bill, completeness_pct]
    })

    # --- Store merged for Accuracy ---
    system_dfs["merged_data"] = merged

    return merged, summary
