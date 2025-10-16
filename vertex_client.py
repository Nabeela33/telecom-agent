from google.cloud import aiplatform

class VertexAgent:
    def __init__(self, project_id, region, model_name="gemini-2.5-flash"):
        self.project_id = project_id
        self.region = region
        self.model_name = model_name
        aiplatform.init(project=project_id, location=region)
        self.model = aiplatform.TextGenerationModel.from_pretrained(model_name)

    def prompt_to_sql(self, prompt: str, mapping_file_name: str, mapping_df, all_datasets):
        # Detect dataset from mapping file
        system_name = mapping_file_name.split("_")[0].lower()
        dataset = next((ds for ds in all_datasets if system_name in ds.lower()), all_datasets[0])

        context = f"""
        You are an expert SQL generator for BigQuery.
        Use dataset `{dataset}` for tables referenced in this mapping.
        Mapping sample: {mapping_df.head(5).to_dict()}
        Convert this user prompt into a valid BigQuery SQL:
        {prompt}
        """
        response = self.model.predict(context)
        sql_query = response.text.strip()
        return sql_query
