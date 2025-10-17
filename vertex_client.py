from google.cloud import aiplatform

class VertexAgent:
    def __init__(self, project_id, region, model_name="gemini-2.5-flash"):
        self.project_id = project_id
        self.region = region
        self.model_name = model_name

        aiplatform.init(project=project_id, location=region)

        # Correct for latest SDK:
        self.model = aiplatform.TextGenerationModel.from_pretrained(model_name)
