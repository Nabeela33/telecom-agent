# vertex_client.py
import vertexai
from vertexai.preview.generative_models import GenerativeModel

class VertexAgent:
    """
    Uses Gemini 2.5 model from Vertex AI to generate SQL queries.
    """

    def __init__(self, project_id: str, region: str, model_name: str = "gemini-2.5-flash"):
        self.project_id = project_id
        self.region = region
        self.model_name = model_name

        # Initialize Vertex AI environment
        vertexai.init(project=project_id, location=region)
        self.model = GenerativeModel(model_name)

    def prompt_to_sql(self, prompt: str, siebel_mapping, antillia_mapping):
        """
        Generate SQL query from a natural language prompt using Gemini 2.5.
        """
        context = f"""
        You are an expert telecom data analyst.
        Convert the following natural language request into a valid BigQuery SQL query.

        Use these mappings as context:
        - Siebel Mapping (sample): {siebel_mapping.head(5).to_dict()}
        - Antillia Mapping (sample): {antillia_mapping.head(5).to_dict()}

        Output ONLY the SQL query without extra commentary.
        User request: {prompt}
        """

        response = self.model.generate_content(context)
        return response.text.strip()
