from google.cloud import bigquery
import pandas as pd

class BigQueryAgent:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def execute(self, query: str) -> pd.DataFrame:
        query_job = self.client.query(query)
        df = query_job.result().to_dataframe()
        return df
