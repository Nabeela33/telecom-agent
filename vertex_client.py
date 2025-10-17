from google.cloud import aiplatform

class VertexAgent:
    def __init__(self, project_id: str, region: str):
        aiplatform.init(project=project_id, location=region)
        # Gemini 2.5 prebuilt model
        self.model = aiplatform.generation.TextGenerationModel.from_pretrained("text-bison@002")

    def prompt_to_sql(self, prompt: str, siebel_mapping, antillia_mapping) -> str:
        # Simple text generation using Gemini
        response = self.model.predict(
            f"Generate SQL based on prompt:\n{prompt}\nUse these mappings: {siebel_mapping.columns.tolist()} & {antillia_mapping.columns.tolist()}"
        )
        return response.text
