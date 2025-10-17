from vertexai import init
from vertexai.generative_models import GenerativeModel

class VertexAgent:
    def __init__(self, project_id, region):
        init(project=project_id, location=region)
        self.model = GenerativeModel("gemini-1.5-pro")  # Gemini 2.5 compatible API

    def prompt_to_sql(self, prompt, siebel_mapping, antillia_mapping):
        """Generate SQL query from natural-language prompt using Gemini."""
        context = f"""
        You are an expert SQL generator for telecom datasets.
        Use the following table mappings to infer dataset and field names.

        Siebel Mapping:
        {siebel_mapping.head(10).to_string(index=False)}

        Antillia Mapping:
        {antillia_mapping.head(10).to_string(index=False)}

        Rules:
        - Always generate valid BigQuery SQL.
        - Use backticks around fully-qualified table names.
        - Only reference tables that exist in the dataset names that contain the mapping's system name.
        - Do not include explanations, return only SQL.
        """

        response = self.model.generate_content(f"{context}\n\nUser Query: {prompt}\n\nSQL:")
        return response.text.strip()
