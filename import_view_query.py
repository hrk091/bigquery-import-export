import os
import shutil

from google.cloud import bigquery

QUERY_DIR = 'query'
try:
    DATASET_ID_LIST = os.environ['DATASETS'].split(',')
except KeyError:
    raise RuntimeError("Environment Variable `DATASETS` must be set for view query import.")
TABLE_ID = os.environ.get('TABLE')

client = bigquery.Client()


def main():
    views = list()
    for dataset_id in DATASET_ID_LIST:
        table_iter = client.list_tables(dataset_id)
        views.extend([table for table in table_iter if table.table_type == 'VIEW'])

    should_replace_all_queries = not TABLE_ID
    if should_replace_all_queries:
        remove_old_queries()

    for view in views:
        if TABLE_ID and view.table_id != TABLE_ID:
            continue
        table = client.get_table(view)
        filepath = f"{QUERY_DIR}/{table.dataset_id}/{table.table_id}.sql"
        save_query(filepath, table.view_query)


def remove_old_queries():
    """
    Delete all existing view queries to avoid phantom cache.
    """
    for dataset_id in DATASET_ID_LIST:
        dirpath = f"{QUERY_DIR}/{dataset_id}"
        print(f"Delete all queries: {dirpath}")
        shutil.rmtree(dirpath, ignore_errors=True)


def save_query(filepath: str, query: str):
    """Save new view query"""
    print(f"Sync new query from bq: {filepath}")
    dirname = os.path.dirname(filepath)
    os.makedirs(dirname, exist_ok=True)
    if os.path.exists(filepath):
        os.remove(filepath)
    with open(filepath, 'w') as file:
        file.write(query)


if __name__ == '__main__':
    main()
