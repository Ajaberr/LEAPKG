import json

import weaviate
# Only import what you actually use
from weaviate.classes.init import Auth
from weaviate.classes.config import Integrations
from weaviate.classes.query import QueryReference

WEAVIATE_URL = "https://4fb4akb1sbslkqrsukp7gw.c0.us-west3.gcp.weaviate.cloud"
WEAVIATE_API_KEY = "yXBHsRdc780XaPINTDtMkQSXOMeiCbx2NBEO"
COHERE_API_KEY = "5D4GH4KbkFYP5JT9AFXRWgZXej7TOAxgG5xELkwF"  # optional: set if you want Cohere vectorization


# 1) Connect to Weaviate Cloud
client = weaviate.connect_to_weaviate_cloud(
    cluster_url=WEAVIATE_URL,
    auth_credentials=Auth.api_key(WEAVIATE_API_KEY),
)

# 2) Configure Cohere integration (optional)
client.integrations.configure([
    Integrations.cohere(api_key=COHERE_API_KEY),
])

# 3) Get the 'Dataset' collection handle
dataset = client.collections.get("Dataset")

# 4) Fetch objects with references
"""obj_with_refs = dataset.query.fetch_objects(
    limit=100000,
    return_references=QueryReference(
        link_on="hasDataCategory",
        return_properties=["summary"]
    )
)"""

results = dataset.query.near_text(
        near_text="Precipitation in 1998",
        limit=5,
        return_metadata=weaviate.QueryReturn(distance=True)
    )
        

def to_json_compatible(value):
    """
    Recursively convert any Weaviate custom objects (e.g. _CrossReference, custom classes)
    to normal Python types (dict, list, str, etc.) suitable for JSON serialization.
    """
    # If it's a list, convert each element.
    if isinstance(value, list):
        return [to_json_compatible(v) for v in value]
    
    # If it's a dict, convert each key/value.
    if isinstance(value, dict):
        return {k: to_json_compatible(v) for k, v in value.items()}
    
    # If it's a cross-reference, turn it into a dict or string
    # (depending on what you actually need).
    if value.__class__.__name__ == "_CrossReference":
        # Try to see if `.to_dict()` or `.beacon` attributes exist.
        # If not, you can just convert to a string or a minimal dict.
        return str(value)  # or { "beacon": value.beacon }
    
    # If there's a .to_dict() method, use it
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return to_json_compatible(value.to_dict())
    
    # Fallback: return as-is (int, float, str, bool, etc.)
    return value

json_ready_data = []

for obj in obj_with_refs.objects:
    # Convert each top-level field
    item = {
        "uuid": str(obj.uuid),
        "collection": obj.collection,
        "properties": to_json_compatible(obj.properties),
        "references": to_json_compatible(obj.references),
    }
    json_ready_data.append(item)

# Now item contains only builtin Python types
# => safe to dump to JSON
"""with open("weaviate_objects.json", "w", encoding="utf-8") as f:
    json.dump(json_ready_data, f, ensure_ascii=False, indent=4)
"""

client.close()