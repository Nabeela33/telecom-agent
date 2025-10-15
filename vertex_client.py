# vertex_client.py

import openai

class VertexAgent:
    """
    Replacement for Vertex AI agent using OpenAI GPT.
    Converts natural language prompts to SQL.
    """
    def __init__(self, api_key: str, model: str = "gpt-4"):
        self.api_key = api_key
        self.model = model
        openai.api_key = self.api_key

    def prompt_to_sql(self, prompt: str, siebel_mapping, antillia_mapping):
        """
        Generate SQL from natural language prompt.
        Uses mapping data as context for better accuracy.
        """
        # Combine mappings as context
        context = f"Siebel mappings: {siebel_mapping.head(5).to_dict()}\n"
        context += f"Antillia mappings: {antillia_mapping.head(5).to_dict()}\n"
        full_prompt = f"{context}\nConvert this natural language query into SQL:\n{prompt}"

        response = openai.ChatCompletion.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a SQL expert. Generate accurate SQL queries."},
                {"role": "user", "content": full_prompt}
            ],
            max_tokens=500,
            temperature=0
        )
        sql_query = response.choices[0].message.content.strip()
        return sql_query
