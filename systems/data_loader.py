from bigquery_client import BigQueryAgent

def fetch_system_data(project_id, systems):
    """Fetch all system tables dynamically based on system list."""
    bq = BigQueryAgent(project_id)
    system_dfs = {}

    for system in systems:
        if system.lower() == "siebel":
            system_dfs["siebel_accounts"] = bq.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_accounts`")
            system_dfs["siebel_assets"] = bq.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_assets`")
            system_dfs["siebel_orders"] = bq.execute("SELECT * FROM `telecom-data-lake.o_siebel.siebel_orders`")
        elif system.lower() == "antillia":
            system_dfs["billing_accounts"] = bq.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_accounts`")
            system_dfs["billing_products"] = bq.execute("SELECT * FROM `telecom-data-lake.gibantillia.billing_products`")

    return system_dfs
