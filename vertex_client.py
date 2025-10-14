from google.cloud import aiplatform

class VertexAgent:
    def __init__(self, project_id, region, endpoint_id):
        aiplatform.init(project=project_id, location=region)
        self.endpoint = aiplatform.Endpoint(endpoint_id)
    
    def prompt_to_sql(self, prompt, siebel_mapping=None, antillia_mapping=None):
        """
        Convert natural language prompt to SQL using Vertex AI
        """
        system_msg = "You are a telecom data analyst. Use the provided schema mapping files to generate accurate SQL for BigQuery."
        
        # Include mapping summaries in user message if available
        user_msg = prompt
        if siebel_mapping is not None:
            user_msg += f"\n\nSiebel Mapping:\n{siebel_mapping.head(5).to_string()}"
        if antillia_mapping is not None:
            user_msg += f"\n\nAntillia Mapping:\n{antillia_mapping.head(5).to_string()}"
        
        response = self.endpoint.predict(instances=[{"content": user_msg}])
        sql_query = response.predictions[0]["content"]
        return sql_query
