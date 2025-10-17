from google.cloud import aiplatform

class VertexAgent:
    def __init__(self, project_id, region):
        self.project_id = project_id
        self.region = region

        # Initialize Vertex AI
        aiplatform.init(project=self.project_id, location=self.region)

        # Load Gemini/Bison model
        self.model = aiplatform.generation.TextGenerationModel.from_pretrained(
            "text-bison@001"  # or your Gemini model
        )

    def prompt_to_sql(self, prompt, siebel_mapping, antillia_mapping):
        # Combine prompt with mapping info
        full_prompt = f"Siebel mapping: {siebel_mapping}\nAntillia mapping: {antillia_mapping}\nUser query: {prompt}\nGenerate SQL query:"
        response = self.model.predict(full_prompt)
        return response.text
