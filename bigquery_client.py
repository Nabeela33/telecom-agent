from google.cloud import bigquery

class BigQueryAgent:
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def list_datasets(self):
        """Return list of dataset names in project"""
        return [ds.dataset_id for ds in self.client.list_datasets()]

    def execute(self, sql_query):
        """Execute SQL in BigQuery and return pandas DataFrame"""
        query_job = self.client.query(sql_query)
        results = query_job.result()
        return results.to_dataframe()
