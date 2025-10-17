from google.cloud import bigquery

class BigQueryAgent:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)

    def execute(self, query):
        """Execute SQL and return DataFrame."""
        job = self.client.query(query)
        return job.result().to_dataframe()
