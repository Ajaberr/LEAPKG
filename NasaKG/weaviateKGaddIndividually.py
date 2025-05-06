import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Integrations, Configure
import json
import time
import sys

# Set up error handling
def handle_error(message, exception=None):
    print(f"ERROR: {message}")
    if exception:
        print(f"Exception details: {str(exception)}")
    return

# Connect to your Weaviate Cloud instance
try:
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url="https://0cvx1wnwryurzvn6anyrlg.c0.us-east1.gcp.weaviate.cloud",
        auth_credentials=Auth.api_key("0Tt4MJfcTeCOShjQWIwn44o0jt6cmXhSYPLh"),
    )

    # Configure integrations (Cohere)
    integrations = [
        Integrations.cohere(api_key="iLWviyJ9RpGnXkiLp9YFkMv8evx4YJKMPSbL5wnh"),
    ]
    client.integrations.configure(integrations)
    
except Exception as e:
    handle_error("Failed to connect to Weaviate or configure integrations", e)
    sys.exit(1)

# Load your data files
try:
    with open("cmr_final_data_individual.json", "r", encoding="utf-8") as f:
        cmr_final = json.load(f)
except Exception as e:
    handle_error("Failed to load JSON data", e)
    client.close()
    sys.exit(1)

# Define collection names - ensure these match exactly with what's in Weaviate
storages = [
    "dataset",
    "dataCategory",
    "dataFormat",
    "locationCategory",
    "spatialExtent",
    "station",
    "relationship",
]

batch_size = 1900  # Reduced from 1000 to avoid hitting limits
delay_seconds = 1  
error_threshold = 5

def process_batch(data_list, data_type):
    total_objects = len(data_list)
    print(f"Total objects to process: {total_objects}")

    # First, ensure all collections exist
    for store in storages:
        try:
            # Check if collection exists
            collection_exists = False
            try:
                # Try to get the collection first
                existing_collection = client.collections.get(store)
                if existing_collection:
                    print(f"Collection '{store}' already exists")
                    collection_exists = True
            except Exception as e:
                # Collection likely doesn't exist
                print(f"Collection '{store}' not found, will create it: {str(e)}")
                
            # Create collection if it doesn't exist
            if not collection_exists:
                print(f"Creating collection '{store}'...")
                client.collections.create(
                    name=store,
                    vectorizer_config=Configure.Vectorizer.text2vec_cohere(),
                    generative_config=Configure.Generative.cohere()
                )
                print(f"Collection '{store}' created successfully")
                
        except Exception as e:
            handle_error(f"Failed to create or access collection '{store}'", e)
            return

    # Now process the data
    for i in range(0, total_objects, batch_size):
        current_batch = data_list[i:i+batch_size]
        print(f"Processing {data_type} objects {i+1} to {i+len(current_batch)} of {total_objects}...")

        for store in storages:
            try:
                storage_collection = client.collections.get(store)
                
                print(f"Processing batch for '{store}' collection...")
                with storage_collection.batch.dynamic() as batch:
                    errors_in_batch = 0

                    for item in current_batch:
                        data_to_store = item.get(store)

                        if data_to_store:
                            # Debug output
                            # Explicitly handle polygons to convert them into string format
                            if store == "spatialExtent" and "polygons" in data_to_store:
                                data_to_store["polygons"] = [
                                    str(polygon) for polygon in data_to_store["polygons"]
                                ]

                            try:
                                batch.add_object(data_to_store)
                            except Exception as e:
                                print(f"Error adding object: {str(e)}")
                                errors_in_batch += 1

                            if batch.number_errors > error_threshold:
                                print(f"Stopped '{store}' batch due to excessive errors: {batch.number_errors}")
                                errors_in_batch = batch.number_errors
                                break

                    if errors_in_batch > error_threshold:
                        print(f"Stopping processing due to errors in '{store}'")
                        return
                    
                    print(f"Completed batch for '{store}' with {batch.number_errors} errors")
                
            except Exception as e:
                handle_error(f"Error processing '{store}' collection", e)
                return

        print(f"Batch processed. Waiting {delay_seconds} seconds...")
        time.sleep(delay_seconds)

try:
    print("Starting batch import of CMR data...")
    process_batch(cmr_final, "CMR")
    print("Import completed successfully")
except Exception as e:
    handle_error("Failed during batch processing", e)
finally:
    # Always close the client to avoid resource warnings
    try:
        print("Closing Weaviate client...")
        client.close()
        print("Client closed successfully")
    except:
        print("Error closing client")