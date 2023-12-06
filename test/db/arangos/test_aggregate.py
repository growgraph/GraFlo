from graph_cast.db import ConnectionManager


def test_csv(create_db, conn_conf, current_path, test_db_name):
    _ = create_db

    conn_conf.database = test_db_name
    docs = [
        {"class": "a", "value": 1},
        {"class": "a", "value": 2},
        {"class": "a", "value": 3},
        {"class": "b", "value": 4},
        {"class": "b", "value": 5},
    ]
    with ConnectionManager(connection_config=conn_conf) as db_client:
        r = db_client.upsert_docs_batch(docs, "samples")
