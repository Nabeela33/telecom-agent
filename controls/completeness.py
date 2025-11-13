import pandas as pd

def run_completeness(system_dfs, selected_product):
    """
    Completeness Control:
    - Verifies data consistency between Siebel and Antillia systems
    - Computes Happy Path, Service No Bill, and Bill No Service KPIs
    """
    accounts = system_dfs.get("siebel_accounts")
    assets = system_dfs.get("siebel_assets")
    orders = system_dfs.get("siebel_orders")
    billing_accounts = system_dfs.get("billing_accounts")
    billing_products = system_dfs.get("billing_products")

    # --- Ensure required keys exist ---
    for name, df in {
        "siebel_accounts": accounts,
        "siebel_assets": assets,
        "siebel_orders": orders,
        "billing_accounts": billing_accounts,
        "billing_products": billing_products,
    }.items():
        if df is None:
            raise ValueError(f"Missing dataset: {name}")

    # --- Rename key columns safely ---
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

    # --- Merge Logic (consistent with your app.py flow) ---
    merged = (
        billing_products.merge(
            billing_accounts,
            left_on="billing_account_id_bp",
            right_on="billing_account_id_bacc",
            how="left"
        )
        .merge(
            accounts,
            left_on="billing_account_siebel_account_id",
            right_on="siebel_account_id",
            how="left"
        )
        .merge(
            assets,
            left_on=["asset_id"],
            right_on=["asset_id"],
            how="left"
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

    # --- KPI Computation ---
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
        "KPI",
        "service_no_bill",
        "no_service_bill"
    ]].drop_duplicates()

    return merged, result_df
