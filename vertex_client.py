from vertexai import init
from vertexai.generative_models import GenerativeModel
import time

class VertexAgent:
    def __init__(self, project_id: str, region: str):
        init(project=project_id, location=region)
        self.model = GenerativeModel("gemini-2.5-flash")

    def prompt_to_sql(self, user_prompt: str, siebel_mapping: str, antillia_mapping: str) -> str:
        system_instruction = (
            "You are an expert data analyst who writes optimized BigQuery SQL. "
            "Use the provided mappings to interpret business terms and produce only SQL output."
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

        for _ in range(3):
            try:
                response = self.model.generate_content(prompt)
                return response.text.strip()
            except Exception:
                time.sleep(2)
        raise RuntimeError("Vertex AI SQL generation failed after retries.")
