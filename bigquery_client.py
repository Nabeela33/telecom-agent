from google.cloud import bigquery

class BigQueryAgent:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
    
    def execute(self, sql_query):
        query_job = self.client.query(sql_query)
        return query_job.result().to_dataframe()
