from vertexai import init
from vertexai.generative_models import GenerativeModel

class VertexAgent:
    def __init__(self, project_id: str, region: str):
        init(project=project_id, location=region)
        self.model = GenerativeModel("gemini-2.5-flash")  # No endpoint needed

    def prompt_to_sql(self, user_prompt: str, siebel_mapping: str, antillia_mapping: str) -> str:
        system_instruction = (
            "You are an expert data analyst who writes correct and optimized BigQuery SQL queries. "
            "Use the provided mappings between business terms and actual BigQuery tables/columns. "
            "Infer which system the user is referring to (Siebel or Antillia) from context and mapping file name. "
            "Always format output as a valid SQL query only — no explanations or markdown."
        )

        prompt = f"""
        {system_instruction}

        --- SIEBEL MAPPING ---
        {siebel_mapping[:2000]}

        --- ANTILLIA MAPPING ---
        {antillia_mapping[:2000]}

        --- USER PROMPT ---
        {user_prompt}
        """

        response = self.model.generate_content(prompt)
        return response.text.strip()
