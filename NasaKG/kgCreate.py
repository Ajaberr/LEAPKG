import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Integrations
import random

# Connect to your Weaviate Cloud instance
client = weaviate.connect_to_weaviate_cloud(
    cluster_url="",
    auth_credentials=Auth.api_key(""),
)

# Configure integrations (Cohere)
integrations = [
    Integrations.cohere(api_key=""),
]
client.integrations.configure(integrations)

# Define the collections to work with
collections = [
    "dataset",
    "dataCategory",
    "dataFormat",
    "locationCategory",
    "spatialExtent",
    "station",
    "relationship",
]

# Store all UUIDs by collection and by object name
uuid_by_collection = {}
uuid_by_name = {}

# First, collect all UUIDs and build lookup dictionaries
print("Collecting UUIDs from all collections...")
for collection_name in collections:
    collection = client.collections.get(collection_name)
    uuid_by_collection[collection_name] = []
    
    print(f"Processing collection: {collection_name}")
    for item in collection.iterator():
        uuid_by_collection[collection_name].append(item.uuid)
        
        # If the item has a name property, use it for lookup
        if hasattr(item, 'properties') and item.properties.get('name'):
            if collection_name not in uuid_by_name:
                uuid_by_name[collection_name] = {}
            uuid_by_name[collection_name][item.properties['name']] = item.uuid

print(f"Collected UUIDs for {len(collections)} collections")

# Define relationship mapping with source collection, property name, and target collection
relationship_mapping = {
    "hasDataCategory": ("dataset", "dataCategory"),
    "hasDataFormat": ("dataset", "dataFormat"),
    "hasLocationCategory": ("dataset", "locationCategory"),
    "hasLocation": ("dataset", "spatialExtent"),
    "hasStation": ("dataset", "station"),
    "hasTemporalExtent": ("dataset", "spatialExtent"),
    "includesDataset": ("dataCategory", "dataset"),
    "relatedTo": ("dataCategory", "dataCategory"),
    "hasSubCategory": ("dataCategory", "dataCategory"),
    "usedByDataset": ("dataFormat", "dataset"),
    "compatibleWith": ("dataFormat", "dataFormat"),
    "includesLocation": ("locationCategory", "spatialExtent"),
    "hasParentCategory": ("locationCategory", "locationCategory"),
    "locatedIn": ("spatialExtent", "locationCategory"),
    "containsStation": ("spatialExtent", "station"),
    "adjacentTo": ("spatialExtent", "spatialExtent"),
    "partOf": ("spatialExtent", "spatialExtent"),
    "locatedAt": ("station", "spatialExtent"),
    "operatesDataset": ("station", "dataset"),
    "associatedWithDataset": ("spatialExtent", "dataset"),
    "overlapsWith": ("spatialExtent", "spatialExtent"),
    "definesPeriodFor": ("spatialExtent", "spatialExtent"),
}

# Function to create a relationship between two objects
def create_relationship(source_collection, source_uuid, relation_name, target_collection, target_uuid):
    relationship_collection = client.collections.get("relationship")
    
    # Create a new relationship object
    relationship_properties = {
        "name": f"{relation_name}_{random.randint(1000, 9999)}",
        "type": relation_name,
        relation_name: [{"beacon": f"weaviate://tmjfdmmqooffk4qzw7xvg.c0.us-west3.gcp.weaviate.cloud/{target_collection}/{target_uuid}"}]
    }
    
    # Add source reference
    source_ref_name = f"from{source_collection.capitalize()}"
    relationship_properties[source_ref_name] = [{"beacon": f"weaviate://tmjfdmmqooffk4qzw7xvg.c0.us-west3.gcp.weaviate.cloud/{source_collection}/{source_uuid}"}]
    
    # Create the relationship
    try:
        relationship_collection.data.insert(relationship_properties)
        print(f"Created relationship: {source_collection} -> {relation_name} -> {target_collection}")
        return True
    except Exception as e:
        print(f"Error creating relationship: {e}")
        return False

# For demonstration purposes, create sample relationships based on your examples


# Function to create relationships bidirectionally
def create_bidirectional_relationships():
    print("Creating bidirectional relationships...")
    
    # Define inverse relationship pairs
    inverse_relationships = {
        "hasDataCategory": "includesDataset",
        "hasDataFormat": "usedByDataset",
        "hasLocationCategory": "includesLocation",
        "hasLocation": "locatedIn",
        "hasStation": "operatesDataset",
        "hasTemporalExtent": "associatedWithDataset",
        "relatedTo": "relatedTo",  # Symmetric
        "hasSubCategory": "hasParentCategory",
        "compatibleWith": "compatibleWith",  # Symmetric
        "adjacentTo": "adjacentTo",  # Symmetric
        "partOf": "containsStation",
        "overlapsWith": "overlapsWith",  # Symmetric
    }
    
    # Get all existing relationships
    relationship_collection = client.collections.get("relationship")
    relationships = list(relationship_collection.iterator())
    
    for rel in relationships:
        if not hasattr(rel, 'properties') or not rel.properties.get('type'):
            continue
            
        rel_type = rel.properties['type']
        
        # Skip if this relationship type doesn't have an inverse
        if rel_type not in inverse_relationships:
            continue
            
        inverse_type = inverse_relationships[rel_type]
        
        # Find source and target objects
        source_collection = None
        source_uuid = None
        target_collection = None
        target_uuid = None
        
        # Find the source reference (fromX property)
        for prop, value in rel.properties.items():
            if prop.startswith("from") and isinstance(value, list) and len(value) > 0:
                if 'beacon' in value[0]:
                    beacon = value[0]['beacon']
                    parts = beacon.split('/')
                    if len(parts) >= 5:
                        source_collection = parts[-2]
                        source_uuid = parts[-1]
        
        # Find the target reference (relation property)
        if rel_type in rel.properties and isinstance(rel.properties[rel_type], list) and len(rel.properties[rel_type]) > 0:
            if 'beacon' in rel.properties[rel_type][0]:
                beacon = rel.properties[rel_type][0]['beacon']
                parts = beacon.split('/')
                if len(parts) >= 5:
                    target_collection = parts[-2]
                    target_uuid = parts[-1]
        
        # If we have both source and target, create the inverse relationship
        if source_collection and source_uuid and target_collection and target_uuid:
            # Check if the inverse relationship exists
            inverse_exists = False
            for check_rel in relationships:
                if not hasattr(check_rel, 'properties') or not check_rel.properties.get('type'):
                    continue
                
                if check_rel.properties['type'] != inverse_type:
                    continue
                    
                # Check if this relationship connects the right objects in the right direction
                has_correct_source = False
                has_correct_target = False
                
                # Check source
                for prop, value in check_rel.properties.items():
                    if prop.startswith("from") and isinstance(value, list) and len(value) > 0:
                        if 'beacon' in value[0]:
                            beacon = value[0]['beacon']
                            if target_uuid in beacon and target_collection in beacon:
                                has_correct_source = True
                
                # Check target
                if inverse_type in check_rel.properties and isinstance(check_rel.properties[inverse_type], list) and len(check_rel.properties[inverse_type]) > 0:
                    if 'beacon' in check_rel.properties[inverse_type][0]:
                        beacon = check_rel.properties[inverse_type][0]['beacon']
                        if source_uuid in beacon and source_collection in beacon:
                            has_correct_target = True
                
                if has_correct_source and has_correct_target:
                    inverse_exists = True
                    break
            
            # Create the inverse relationship if it doesn't exist
            if not inverse_exists:
                create_relationship(target_collection, target_uuid, inverse_type, source_collection, source_uuid)

# Main execution
def main():
    
    
    # Option 2: Create bidirectional relationships
    create_bidirectional_relationships()
    
    print("Completed creating relationships")
    client.close()

if __name__ == "__main__":
    main()