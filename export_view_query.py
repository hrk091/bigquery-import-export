import os

from google.cloud import bigquery
import google.auth


_, PROJECT_ID = google.auth.default()
DATASET_ID_LIST = os.environ.get("DATASETS", "").split(",")
TABLE_ID = os.environ.get("TABLE")

QUERY_DIR = "query"
DATASET_ALL = [item for item in os.listdir(QUERY_DIR) if os.path.isdir(f"{QUERY_DIR}/{item}")]
DATASET_ID_LIST = DATASET_ID_LIST if DATASET_ID_LIST[0] else DATASET_ALL

client = bigquery.Client()


def main():
    view_name_list = list()
    for dataset_id in DATASET_ID_LIST:
        view_name_list.extend(
            [(dataset_id, view_file.replace(".sql", "")) for view_file in os.listdir(f"{QUERY_DIR}/{dataset_id}")]
        )

    for dataset_id, view_name in view_name_list:
        if TABLE_ID and view_name != TABLE_ID:
            continue
        query = load_query(dataset_id, view_name)
        upload_view_query(dataset_id, view_name, query)


def load_query(dataset_id: str, view_name: str) -> str:
    """Get local stored view query"""
    with open(f"{QUERY_DIR}/{dataset_id}/{view_name}.sql", "r") as file:
        view_query = file.read()
    return view_query


def upload_view_query(dataset_id: str, view_name: str, query: str):
    """
    Upload local stored view query to bigquery
    Only update existing view is available ( creating new view query cannot be performed. )
    """
    table_path = f"{PROJECT_ID}.{dataset_id}.{view_name}"
    print(f"Sync new query to bq: {table_path}")
    table = bigquery.Table(table_path)
    table.view_query = query
    client.update_table(table, ["view_query"])



if __name__ == "__main__":
    main()
