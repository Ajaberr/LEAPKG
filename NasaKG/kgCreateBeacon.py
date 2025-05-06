import json
import time
import sys
import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import (
    Integrations, Configure,
    DataType, Property, ReferenceProperty
)
from weaviate.classes.data import DataReference
from weaviate.classes.query import QueryReference
import uuid


##############################
#  CONFIG
##############################
WEAVIATE_URL = ""
WEAVIATE_API_KEY = ""
COHERE_API_KEY = ""  # optional: set if you want Cohere vectorization
DATA_FILE = "cmr_final_data_individual.json"

# The nine NASAClimateKG “collections” (equivalent to classes in older versions)
storages = [
    "DataCategory",
    "DataFormat",
    "LocationCategory",
    "SpatialExtent",
    "Station",
    "TemporalExtent",
    "Duration",
    "Relationship",
    "Dataset"
]

def connect_to_weaviate():
    """
    Connect to a Weaviate 4.x cluster with an API key,
    optionally configure the Cohere vectorizer integration.
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client = weaviate.connect_to_weaviate_cloud(
                cluster_url=WEAVIATE_URL,
                auth_credentials=Auth.api_key(WEAVIATE_API_KEY),
            )
            # If using Cohere:
            if COHERE_API_KEY:
                integrations = [Integrations.cohere(api_key=COHERE_API_KEY)]
                client.integrations.configure(integrations)

            return client
        except Exception as e:
            print(f"Connection attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise

def delete_and_create_collections(client: weaviate.Client):
    """
    Delete (if exist) and re-create the nine NASAClimateKG collections.
    Comment out if you don't want to wipe data each time.
    """

    # 1) Delete if existing
    client.collections.delete_all()

    # 2) Create them.
    print("Creating DataCategory collection...")
    client.collections.create(
        "DataCategory",
        properties=[
            Property(name="summary", data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

    print("Creating DataFormat collection...")
    client.collections.create(
        "DataFormat",
        properties=[
            Property(name="original_format", data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

    print("Creating LocationCategory collection...")
    client.collections.create(
        "LocationCategory",
        properties=[
            Property(name="category", data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

    print("Creating SpatialExtent collection...")
    client.collections.create(
        "SpatialExtent",
        properties=[
            Property(name="boxes",         data_type=DataType.TEXT),
            Property(name="polygons",      data_type=DataType.TEXT),
            Property(name="points",        data_type=DataType.TEXT),
            Property(name="place_names",   data_type=DataType.TEXT),
            Property(name="time_start",    data_type=DataType.TEXT),
            Property(name="time_end",      data_type=DataType.TEXT),
            Property(name="duration_days", data_type=DataType.INT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

    print("Creating Station collection...")
    client.collections.create(
        "Station",
        properties=[
            Property(name="platforms", data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

    print("Creating TemporalExtent collection...")
    client.collections.create(
        "TemporalExtent",
        properties=[
            Property(name="start_time", data_type=DataType.TEXT),
            Property(name="end_time",   data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

    print("Creating Duration collection...")
    client.collections.create(
        "Duration",
        properties=[
            Property(name="days", data_type=DataType.INT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

    print("Creating Relationship collection...")
    client.collections.create(
        "Relationship",
        # no additional properties, or add them if needed
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

    print("Creating Dataset collection...")
    client.collections.create(
        "Dataset",
        properties=[
            Property(name="short_name", data_type=DataType.TEXT),
            Property(name="title",      data_type=DataType.TEXT),
            Property(name="links",      data_type=DataType.TEXT),
        ],
        vectorizer_config=Configure.Vectorizer.text2vec_cohere()
    )

def add_refs(client: weaviate.Client):
    """
    Add references to each collection after creation.
    Must align with the RELATIONSHIP_MAP used for linking.
    """
    dataset = client.collections.get("Dataset")
    dataset.config.add_reference(ReferenceProperty(name="hasDataCategory", target_collection="DataCategory"))
    dataset.config.add_reference(ReferenceProperty(name="hasDataFormat", target_collection="DataFormat"))
    dataset.config.add_reference(ReferenceProperty(name="hasLocationCategory", target_collection="LocationCategory"))
    dataset.config.add_reference(ReferenceProperty(name="hasLocation", target_collection="SpatialExtent"))
    dataset.config.add_reference(ReferenceProperty(name="hasStation", target_collection="Station"))
    dataset.config.add_reference(ReferenceProperty(name="hasTemporalExtent", target_collection="TemporalExtent"))

    # DataCategory
    data_category = client.collections.get("DataCategory")
    data_category.config.add_reference(ReferenceProperty(name="includesDataset", target_collection="Dataset"))
    data_category.config.add_reference(ReferenceProperty(name="relatedTo", target_collection="DataCategory"))
    data_category.config.add_reference(ReferenceProperty(name="hasSubCategory", target_collection="DataCategory"))
    # DataFormat
    data_format = client.collections.get("DataFormat")
    data_format.config.add_reference(ReferenceProperty(name="usedByDataset", target_collection="Dataset"))
    data_format.config.add_reference(ReferenceProperty(name="compatibleWith", target_collection="DataFormat"))



    # LocationCategory
    location_category = client.collections.get("LocationCategory")
    location_category.config.add_reference(ReferenceProperty(name="includesLocation", target_collection="SpatialExtent"))
    location_category.config.add_reference(ReferenceProperty(name="hasParentCategory", target_collection="LocationCategory"))
    # SpatialExtent
    spatial_extent = client.collections.get("SpatialExtent")
    spatial_extent.config.add_reference(ReferenceProperty(name="locatedIn", target_collection="LocationCategory"))
    spatial_extent.config.add_reference(ReferenceProperty(name="containsStation", target_collection="Station"))
    spatial_extent.config.add_reference(ReferenceProperty(name="adjacentTo", target_collection="SpatialExtent"))
    spatial_extent.config.add_reference(ReferenceProperty(name="partOf", target_collection="SpatialExtent"))
    # Station
    station = client.collections.get("Station")
    station.config.add_reference(ReferenceProperty(name="locatedAt", target_collection="SpatialExtent"))
    station.config.add_reference(ReferenceProperty(name="operatesDataset", target_collection="Dataset"))
    # TemporalExtent
    temporal_extent = client.collections.get("TemporalExtent")
    temporal_extent.config.add_reference(ReferenceProperty(name="associatedWithDataset", target_collection="Dataset"))
    temporal_extent.config.add_reference(ReferenceProperty(name="hasDuration", target_collection="Duration"))
    temporal_extent.config.add_reference(ReferenceProperty(name="overlapsWith", target_collection="TemporalExtent"))
    # Duration
    duration = client.collections.get("Duration")
    duration.config.add_reference(ReferenceProperty(name="definesPeriodFor", target_collection="TemporalExtent"))
    print("✅ All references added successfully.")

RELATIONSHIP_MAP = {
    # 1) Dataset
    "hasDataCategory":    ("Dataset", "DataCategory"),
    "hasDataFormat":      ("Dataset", "DataFormat"),
    "hasLocationCategory":("Dataset", "LocationCategory"),
    "hasLocation":        ("Dataset", "SpatialExtent"),
    "hasStation":         ("Dataset", "Station"),
    "hasTemporalExtent":  ("Dataset", "TemporalExtent"),

    # 2) DataCategory
    "includesDataset": ("DataCategory", "Dataset"),
    "relatedTo":       ("DataCategory", "DataCategory"),
    "hasSubCategory":  ("DataCategory", "DataCategory"),

    # 3) DataFormat
    "usedByDataset":  ("DataFormat", "Dataset"),
    "compatibleWith": ("DataFormat", "DataFormat"),

    # 4) LocationCategory
    "includesLocation":  ("LocationCategory", "SpatialExtent"),
    "hasParentCategory": ("LocationCategory", "LocationCategory"),

    # 5) SpatialExtent
    "locatedIn":       ("SpatialExtent", "LocationCategory"),
    "containsStation": ("SpatialExtent", "Station"),
    "adjacentTo":      ("SpatialExtent", "SpatialExtent"),
    "partOf":          ("SpatialExtent", "SpatialExtent"),

    # 6) Station
    "locatedAt":       ("Station", "SpatialExtent"),
    "operatesDataset": ("Station", "Dataset"),

    # 7) TemporalExtent
    "associatedWithDataset": ("TemporalExtent", "Dataset"),
    "hasDuration":           ("TemporalExtent", "Duration"),
    "overlapsWith":          ("TemporalExtent", "TemporalExtent"),

    # 8) Duration
    "definesPeriodFor": ("Duration", "TemporalExtent"),
}
def process_batch(data_list, data_type, client: weaviate.Client, uuid_map):
    """
    Insert an object for EVERY class in storages for each item in data_list
    (even if the doc is empty or an insertion error occurs). This preserves strict
    index matching across classes by creating an empty object with a UUID on error.

    We do a per-class check for TARGET_PER_COLLECTION to limit total ingestion.
    """
    TARGET_PER_COLLECTION = 300000 // 9
    batch_size = 1900
    delay_seconds = 1
    error_threshold = 5

    total_docs = len(data_list)
    print(f"Total docs to process: {total_docs}")

    # Initialize uuid_map for each store with a None placeholder for each doc
    for store in storages:
        uuid_map[store] = [None] * total_docs

    for i in range(0, total_docs, batch_size):
        current_batch = data_list[i:i + batch_size]
        print(f"\nProcessing batch for docs {i+1} to {i+len(current_batch)} of {total_docs}...")

        # Handle each class in storages
        for store in storages:
            storage_collection = client.collections.get(store)
            # Skip if we've reached the limit for this store
            current_count = sum(u is not None for u in uuid_map[store])
            if current_count >= TARGET_PER_COLLECTION:
                print(f"[SKIP] '{store}' has reached {TARGET_PER_COLLECTION} objects. Skipping further ingestion.")
                continue

            print(f"  -> Inserting into '{store}' collection...")
            with storage_collection.batch.dynamic() as batch:
                errors_in_batch = 0

                for offset, doc in enumerate(current_batch):
                    doc_index = i + offset

                    # Break if we hit the limit
                    current_count = sum(u is not None for u in uuid_map[store])
                    if current_count >= TARGET_PER_COLLECTION:
                        break

                    # Skip if UUID already assigned for this doc_index in this store
                    if uuid_map[store][doc_index] is not None:
                        continue

                    # Prepare data to store, defaulting to empty dict if not present
                    data_to_store = doc.get(store, {})

                    # Convert array fields to strings where necessary
                    if store == "SpatialExtent":
                        if "polygons" in data_to_store:
                            data_to_store["polygons"] = str(data_to_store["polygons"])
                        if "boxes" in data_to_store:
                            data_to_store["boxes"] = str(data_to_store["boxes"])
                        if "points" in data_to_store:
                            data_to_store["points"] = str(data_to_store["points"])
                        if "place_names" in data_to_store:
                            data_to_store["place_names"] = str(data_to_store["place_names"])

                    if store == "Station" and "platforms" in data_to_store:
                        data_to_store["platforms"] = str(data_to_store["platforms"])

                    if store == "Dataset" and "links" in data_to_store:
                        data_to_store["links"] = str(data_to_store["links"])

                    if store == "Duration" and "days" in data_to_store:
                        try:
                            data_to_store["days"] = int(data_to_store["days"] or 0)
                        except ValueError:
                            data_to_store["days"] = 0  # Default to 0 if conversion fails

                    # Generate a UUID for this object
                    my_uuid = str(uuid.uuid4())

                    try:
                        # Attempt to add the object with its data
                        batch.add_object(data_to_store, uuid=my_uuid)
                        uuid_map[store][doc_index] = my_uuid
                    except Exception as e:
                        # On error, add an empty object with the same UUID
                        print(f"    [ERROR] adding doc_index={doc_index} to '{store}': {e}")
                        try:
                            batch.add_object({}, uuid=my_uuid)
                            uuid_map[store][doc_index] = my_uuid
                            print(f"    [INFO] Added empty object for doc_index={doc_index} in '{store}'")
                        except Exception as e2:
                            # If even the empty object fails, log and count the error
                            print(f"    [ERROR] Failed to add empty object for doc_index={doc_index} in '{store}': {e2}")
                            errors_in_batch += 1
                            if errors_in_batch > error_threshold:
                                print(f"    [STOP] Too many errors in '{store}' batch.")
                                break

                if errors_in_batch > error_threshold:
                    print(f"[STOP] Exiting due to errors in '{store}'.")
                    return

        # Completed one batch across all classes
        print(f"Finished batch. Sleeping {delay_seconds} sec...\n")
        time.sleep(delay_seconds)

    print(f"\nDone inserting data for all classes (or reached target limits).")

def add_object_references(client: weaviate.Client, data_list: list, uuid_map: dict):
    print("\n=== Linking references for all items ===")
    total_docs = len(data_list)
    print(f"Total docs to link references for: {total_docs}")

    coll_map = {}
    for class_name in storages:
        try:
            coll_map[class_name] = client.collections.get(class_name)
        except Exception as e:
            print(f"[ERROR] Could not retrieve collection '{class_name}': {e}")
            coll_map[class_name] = None

    references_linked = 0

    for i in range(total_docs):
        for rel_prop, (from_cls, to_cls) in RELATIONSHIP_MAP.items():
            from_uuid = uuid_map[from_cls][i]
            to_uuid = uuid_map[to_cls][i]
            if from_uuid is None or to_uuid is None:
                continue  # Skip if either object doesn’t exist
            from_collection = coll_map.get(from_cls)
            if from_collection is None:
                continue
            try:
                from_collection.data.reference_add(
                    from_uuid=from_uuid,
                    from_property=rel_prop,
                    to=to_uuid
                )
                references_linked += 1
                if references_linked % 1000 == 0:
                    print(f"  -> Linked {references_linked} references so far...")
            except Exception as e:
                print(f"[ERROR] linking doc {i}, prop='{rel_prop}' from '{from_uuid}' -> '{to_uuid}': {e}")

    print(f"Done linking references. Total references linked: {references_linked}\n")

    
###########################
# OPTIONAL TEST FUNCTION
###########################
def test_small_sample(client: weaviate.Client, full_data: list, sample_size: int = 2):
    """
    Test the ingestion + referencing pipeline with a small subset of data
    on ephemeral classes named *_test. This is just for quick debugging,
    so you don't rewrite the main classes repeatedly.
    """
    sample_data = full_data[:sample_size]
    print(f"\n[TEST] Using a sample of size {sample_size}.\n")

    # We'll define ephemeral class names to avoid overwriting real classes
    test_storages = [
        "dataset_test",
        "dataCategory_test",
        "dataFormat_test",
        "locationCategory_test",
        "spatialExtent_test",
        "station_test",
        "relationship_test",
        "temporalExtent_test",
        "duration_test"
    ]

    # Clear everything first
    print("[TEST] Deleting any existing ephemeral classes...")
    client.collections.delete_all()

    print("[TEST] Creating ephemeral test classes...")
    for c_name in test_storages:
        client.collections.create(
            name=c_name,
            vectorizer_config=Configure.Vectorizer.text2vec_cohere(),
            properties=[
                Property(name="sampleField", data_type=DataType.TEXT)
            ]
        )
        print(f"  -> Created ephemeral class '{c_name}'")

    # We'll define a "uuid_map" for these ephemeral classes
    test_uuid_map = {c_name: [None] * sample_size for c_name in test_storages}

    # Insert the sample data
    print("[TEST] Inserting sample data into ephemeral classes...")
    for c_name in test_storages:
        store_obj = client.collections.get(c_name)
        with store_obj.batch.dynamic() as batch:
            for i, item in enumerate(sample_data):
                # We'll store everything as a JSON dump in "sampleField"
                data_to_store = {
                    "sampleField": json.dumps(item.get(c_name.replace("_test", ""), {}))
                }
                my_uuid = str(uuid.uuid4())
                batch.add_object(data_to_store, uuid=my_uuid)
                test_uuid_map[c_name][i] = my_uuid

    print("[TEST] Inserted sample objects into ephemeral classes.\n")

    # Add a new reference property to 'dataset_test'
    dataset_test_coll = client.collections.get("dataset_test")
    dataCategory_test_coll = client.collections.get("dataCategory_test")

    try:
        print("[TEST] Adding property 'hasTestCategory' to 'dataset_test'...")
        dataset_test_coll.config.add_reference(
            ReferenceProperty(name="hasTestCategory", target_collection="dataCategory_test")
        )
    except Exception as e:
        print(f"[TEST] Could not add property 'hasTestCategory': {e}")
        return

    # Link dataset_test[i] -> dataCategory_test[i]
    for i in range(sample_size):
        ds_uuid = test_uuid_map["dataset_test"][i]
        cat_uuid = test_uuid_map["dataCategory_test"][i]
        if ds_uuid and cat_uuid:
            dataset_test_coll.data.reference_add(
                from_uuid=ds_uuid,
                from_property="hasTestCategory",
                to=cat_uuid
            )
    print("[TEST] Finished linking ephemeral references.\n")

    # Fetch each dataset_test object with references
    for i in range(sample_size):
        ds_uuid = test_uuid_map["dataset_test"][i]
        if not ds_uuid:
            continue
        print(f"[TEST] dataset_test object index={i}, UUID={ds_uuid}:")
        obj_with_refs = dataset_test_coll.query.fetch_objects(
            limit=1,
            return_references=QueryReference(
                link_on="hasTestCategory",
                return_properties=["sampleField"]
            )
        )
        print("    Full data from Weaviate ->", obj_with_refs)
    print("[TEST] Done ephemeral test.\n")


###########################
# MAIN SCRIPT
###########################
def main():
    client = connect_to_weaviate()

    # 1) Delete old & create new collections
    delete_and_create_collections(client)

    # 2) Add references to the schema
    add_refs(client)

    # 3) Load data
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data_list = json.load(f)

    # 4) Insert data + track UUIDs (no skipping, always create an object per doc/store)
    uuid_map = {s: [] for s in storages}
    process_batch(data_list, "CMR", client, uuid_map)

    # 5) Add references for each doc
    add_object_references(client, data_list, uuid_map)

    # 6) Optionally test a small ephemeral sample
    # test_small_sample(client, data_list, sample_size=3)

    print("All tasks complete. Closing client.")
    client.close()


if __name__ == "__main__":
    main()
