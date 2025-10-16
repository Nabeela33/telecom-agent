import json
import pandas as pd

def generate_sql(prompt, siebel_mapping, antillia_mapping):
    """
    Simple rule-based SQL generator using mapping files.
    You can expand rules as needed.
    """
    # Example: if user mentions "orders", map to order table
    sql = f"-- SQL generated for prompt: {prompt}\nSELECT * FROM orders LIMIT 10;"
    return sql

def load_mappings():
    # Load your CSV/XLSX mapping files inside the container
    siebel_mapping = pd.read_csv("siebel_mapping.csv")
    antillia_mapping = pd.read_csv("antillia_mapping.csv")
    return siebel_mapping, antillia_mapping

# Load mappings once at startup
SIEBEL_MAPPING, ANTILLIA_MAPPING = load_mappings()

def predict(request):
    """
    Vertex AI custom prediction handler.
    Expects JSON with 'prompt' key.
    """
    request_json = request.get_json(silent=True)
    prompt = request_json.get("prompt", "")
    
    sql_query = generate_sql(prompt, SIEBEL_MAPPING, ANTILLIA_MAPPING)
    return json.dumps({"sql_query": sql_query})
