from bigquery_client import BigQueryAgent
from utils import load_yaml_config

def fetch_system_data(project_id, bucket_name, system_yaml_file, systems_to_use):
    """Fetch all tables for specified systems using YAML configuration."""
    bq_agent = BigQueryAgent(project_id)
    config = load_yaml_config(bucket_name, system_yaml_file)

    all_dfs = {}
    for sys_name in systems_to_use:
        sys_info = config["systems"].get(sys_name)
        if not sys_info:
            continue
        dataset = sys_info["dataset"]
        for table in sys_info["tables"]:
            short_name = f"{sys_name.lower()}_{table.split('.')[-1]}"
            table_full = f"`{dataset}.{table}`"
            try:
                df = bq_agent.execute(f"SELECT * FROM {table_full}")
                all_dfs[short_name] = df
            except Exception as e:
                print(f"⚠️ Could not load {table_full}: {e}")
    return all_dfs
