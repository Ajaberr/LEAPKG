import requests
import json
import time
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from shapely.ops import unary_union

##############################
#  CONFIG: Shapefile Path
##############################
ADMIN_SHAPEFILE_PATH = "NasaKG/boundaries/boundaries.shp"

##############################
#  (1) Fetch Data
##############################
def fetch_nasa_cmr_all_pages(page_size=200, max_pages=None):
    """
    Fetches dataset 'collections' from NASA's CMR API.
    - page_size: results per page
    - max_pages: optionally limit total pages
    Returns a list of dataset entries.
    """
    cmr_url = "https://cmr.earthdata.nasa.gov/search/collections.json"
    all_data = []
    page_num = 1

    while True:
        params = {
            "page_size": page_size,
            "page_num": page_num
        }
        try:
            response = requests.get(cmr_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # If there's no valid data, stop
            if ("feed" not in data 
                    or "entry" not in data["feed"] 
                    or not data["feed"]["entry"]):
                break

            entries = data["feed"]["entry"]
            all_data.extend(entries)

            print(f"Fetched page {page_num}, total datasets so far: {len(all_data)}")

            page_num += 1
            time.sleep(0.2)  # small delay to avoid rapid requests

            if max_pages and page_num > max_pages:
                break

        except requests.exceptions.Timeout:
            print("Request timed out. Ending fetch loop.")
            break
        except requests.exceptions.RequestException as e:
            print(f"Error fetching NASA CMR data: {e}")
            break

    return all_data


##############################
#  (2) Geometry Helpers
##############################
def extract_polygons(geom):
    """
    Ensure we only return Polygon or MultiPolygon.
    If 'geom' is a GeometryCollection, extract any polygons inside.
    Return None if there's nothing suitable.
    """
    if geom is None:
        return None

    gtype = geom.geom_type
    if gtype in ["Polygon", "MultiPolygon"]:
        return geom
    elif gtype == "GeometryCollection":
        polys = [g for g in geom.geoms if g.geom_type in ["Polygon", "MultiPolygon"]]
        if not polys:
            return None
        if len(polys) == 1:
            return polys[0]
        return unary_union(polys)
    else:
        return None


def parse_cmr_spatial(boxes=None, polygons=None, points=None):
    """
    Convert NASA CMR 'boxes', 'polygons', or 'points' into
    a single Polygon/MultiPolygon if possible.
    Skips or merges geometry as needed.
    """
    shapes = []

    # 1) Boxes -> Polygons
    if boxes:
        for b in boxes:
            coords = b.split()
            if len(coords) == 4:
                # [SouthLat, WestLon, NorthLat, EastLon]
                southLat, westLon, northLat, eastLon = map(float, coords)
                poly = Polygon([
                    (westLon, southLat),
                    (eastLon, southLat),
                    (eastLon, northLat),
                    (westLon, northLat),
                    (westLon, southLat),
                ])
                shapes.append(poly)

    # 2) Polygons
    if polygons:
        for poly_list in polygons:
            for poly_str in poly_list:
                coords = poly_str.split()
                if len(coords) < 6:
                    continue
                pairs = []
                for i in range(0, len(coords), 2):
                    lat = float(coords[i])
                    lon = float(coords[i+1])
                    pairs.append((lon, lat))
                if pairs and pairs[0] != pairs[-1]:
                    pairs.append(pairs[0])
                if len(pairs) > 2:
                    shapes.append(Polygon(pairs))

    # Skipping points in this example

    if not shapes:
        return None
    if len(shapes) == 1:
        merged_geom = shapes[0]
    else:
        merged_geom = unary_union(shapes)

    return extract_polygons(merged_geom)


##############################
#  (3) Classification Helpers
##############################
def classify_bbox_scope(rows_for_dataset):
    """
    Given a set of admin polygons (rows) intersecting a NASA dataset geometry,
    classify bounding box as 'city', 'country', 'continent', or 'global'.
    Return also sets of city/country/continent names found.
    """
    # Adjust column names to your shapefile
    CITY_COL = 'NAME_2'
    COUNTRY_COL = 'ADMIN'
    CONTINENT_COL = 'CONTINENT'

    cities = set()
    countries = set()
    continents = set()

    for _, row in rows_for_dataset.iterrows():
        city_val = row.get(CITY_COL)
        country_val = row.get(COUNTRY_COL)
        continent_val = row.get(CONTINENT_COL)
        if city_val:
            cities.add(city_val)
        if country_val:
            countries.add(country_val)
        if continent_val:
            continents.add(continent_val)

    # Example logic (adjust as needed)
    if len(cities) == 1 and len(countries) == 1:
        scope = 'city'
    elif len(countries) > 1 and len(continents) == 1:
        scope = 'continent'
    elif len(continents) > 1:
        scope = 'global'
    elif len(cities) > 1 or len(countries) == 1:
        scope = 'country'
    else:
        scope = 'city'  # fallback

    return {
        'scope': scope,
        'cities': list(cities),
        'countries': list(countries),
        'continents': list(continents)
    }


##############################
#  (4) Bulk Intersection
##############################
def bulk_find_admin_areas(nasa_gdf, admin_shapefile_path):
    """
    Reads admin shapefile once, does a single spatial join with NASA polygons,
    returns a DataFrame that has columns from both NASA GDF and admin shapefile.
    """
    admin_gdf = gpd.read_file(admin_shapefile_path)

    if nasa_gdf.crs is None:
        nasa_gdf.set_crs(admin_gdf.crs, inplace=True)
    else:
        nasa_gdf = nasa_gdf.to_crs(admin_gdf.crs)

    joined = gpd.sjoin(nasa_gdf, admin_gdf, how="left", predicate="intersects")
    return joined


##############################
#  (5) Main Transformation
##############################
def transform_cmr_to_classes(all_entries):
    """
    1) Returns:
       original_output, individual_output, fail_count

    2) Also includes new 'TemporalExtent' and 'Duration' classes.
    """

    original_output = {
        "Dataset": [],
        "DataCategory": [],
        "DataFormat": [],
        "LocationCategory": [],
        "SpatialExtent": [],
        "Station": [],
        "Relationship": [],
        # NEW:
        "TemporalExtent": [],
        "Duration": []
    }

    individual_output = []
    geoms = []
    fail_count = 0

    for idx, entry in enumerate(all_entries):
        # ---------------------------
        # (1) Existing code: dataset
        # ---------------------------
        dataset_obj = {
            "short_name": entry.get("short_name", "N/A"),
            "title": entry.get("title", "N/A"),
            "links": entry.get("links", [])
        }
        original_output["Dataset"].append(dataset_obj)

        # dataCategory
        data_category_obj = {
            "summary": entry.get("summary", "N/A")
        }
        original_output["DataCategory"].append(data_category_obj)

        # dataFormat
        data_format_obj = {
            "original_format": entry.get("original_format", "N/A")
        }
        original_output["DataFormat"].append(data_format_obj)

        # locationCategory
        location_category_obj = {"category": None}
        original_output["LocationCategory"].append(location_category_obj)

        # spatialExtent
        boxes = entry.get("boxes", [])
        polygons = entry.get("polygons", [])
        points = entry.get("points", [])

        time_start_str = entry.get("time_start")
        time_end_str = entry.get("time_end")

        # Attempt to compute duration days
        duration_days = None
        if time_start_str and time_end_str:
            try:
                start_dt = pd.to_datetime(time_start_str)
                end_dt = pd.to_datetime(time_end_str)
                duration_days = (end_dt - start_dt).days
            except:
                pass

        spatial_extent_obj = {
            "boxes": boxes,
            "polygons": polygons,
            "points": points,
            "place_names": [],
            "time_start": time_start_str,
            "time_end": time_end_str,
            "duration_days": duration_days
        }
        original_output["SpatialExtent"].append(spatial_extent_obj)

        # station
        station_obj = {
            "platforms": entry.get("platforms", [])
        }
        original_output["Station"].append(station_obj)

        # relationship
        relationship_obj = {
            # existing placeholders
            "hasDataCategory": [],
            "hasDataFormat": [],
            # ...
            "definesPeriodFor": []
        }
        original_output["Relationship"].append(relationship_obj)

        # ---------------------------
        # (2) NEW: temporalExtent
        # ---------------------------
        # For demonstration, let's store 'time_start', 'time_end'
        # plus the same "duration_days" logic, or any other fields you want
        temporal_extent_obj = {
            "start_time": time_start_str,
            "end_time": time_end_str
        }
        original_output["TemporalExtent"].append(temporal_extent_obj)

        # ---------------------------
        # (3) NEW: duration
        # ---------------------------
        # We can store 'days' as an int. If we want more detail, we can add it.
        duration_obj = {
            "days": duration_days
        }
        original_output["Duration"].append(duration_obj)

        # ---------------------------
        # (4) Build the individual record
        # ---------------------------
        individual_dataset_dict = {
            "Dataset": dataset_obj,
            "DataCategory": data_category_obj,
            "DataFormat": data_format_obj,
            "LocationCategory": location_category_obj,
            "SpatialExtent": spatial_extent_obj,
            "Station": station_obj,
            "Relationship": relationship_obj,

            # NEW
            "TemporalExtent": temporal_extent_obj,
            "Duration": duration_obj
        }
        individual_output.append(individual_dataset_dict)

        # parse geometry if applicable
        geometry = parse_cmr_spatial(boxes, polygons, points)
        if geometry is None:
            fail_count += 1
            location_category_obj["category"] = "unclassified"
            continue

        geoms.append({"dataset_index": idx, "geometry": geometry})

    # 3) If no valid geometries, return
    if not geoms:
        return original_output, individual_output, fail_count

    # 4) Spatial join & classification
    nasa_gdf = gpd.GeoDataFrame(geoms, geometry="geometry", crs="EPSG:4326")
    joined = bulk_find_admin_areas(nasa_gdf, ADMIN_SHAPEFILE_PATH)
    grouped = joined.groupby("dataset_index")

    for dataset_index, rows in grouped:
        if len(rows) == 1 and pd.isnull(rows.iloc[0]["index_right"]):
            # unclassified
            original_output["LocationCategory"][dataset_index]["category"] = "unclassified"
            original_output["SpatialExtent"][dataset_index]["place_names"] = []
            individual_output[dataset_index]["LocationCategory"]["category"] = "unclassified"
            individual_output[dataset_index]["SpatialExtent"]["place_names"] = []
            continue

        classification = classify_bbox_scope(rows)
        scope = classification["scope"]
        place_names = (
            classification["cities"]
            + classification["countries"]
            + classification["continents"]
        )
        original_output["LocationCategory"][dataset_index]["category"] = scope
        original_output["SpatialExtent"][dataset_index]["place_names"] = place_names
        individual_output[dataset_index]["LocationCategory"]["category"] = scope
        individual_output[dataset_index]["SpatialExtent"]["place_names"] = place_names

    return original_output, individual_output, fail_count


##############################
#  (6) Main
##############################
def main():
    # 1) Fetch NASA CMR data
    all_data = fetch_nasa_cmr_all_pages(page_size=200, max_pages=None)
    print(f"Total collections fetched: {len(all_data)}")

    # 2) Transform & classify
    (
        structured_data_original,
        structured_data_individual,
        fail_count
    ) = transform_cmr_to_classes(all_data)

    # 3) Save the parallel-lists format
    output_file_original = "cmr_final_data.json"
    with open(output_file_original, "w", encoding="utf-8") as f:
        json.dump(structured_data_original, f, indent=2)
    print(f"Saved original-format data to {output_file_original}")

    # 4) Save the individual-records format
    output_file_individual = "cmr_final_data_individual.json"
    with open(output_file_individual, "w", encoding="utf-8") as f:
        json.dump(structured_data_individual, f, indent=2)
    print(f"Saved individual-record data to {output_file_individual}")

    # 5) Print how many datasets had geometry issues
    print(f"{fail_count} datasets had invalid or unsupported geometry.")


if __name__ == "__main__":
    main()
