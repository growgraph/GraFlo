from test.arangos.conftest import ingest_atomic

from graph_cast.architecture import JConfigurator
from graph_cast.db import ConnectionManager
from graph_cast.db.arango.util import get_data_from_cursor
from graph_cast.onto import InputType
from graph_cast.util import ResourceHandler


def test_weights(conn_conf, current_path, test_db_name):
    ingest_atomic(
        conn_conf,
        current_path,
        test_db_name,
        input_type=InputType.JSON,
        mode="kg_v3b",
    )

    collection = "mentions_entities_edges"
    q = f"""FOR doc IN {collection}
                RETURN doc"""

    with ConnectionManager(connection_config=conn_conf) as db_client:
        cursor = db_client.execute(q)
        data = get_data_from_cursor(cursor)

    assert all(["publication@_key" in item for item in data])


def test_compound_index():
    config = ResourceHandler.load(f"conf.json", f"kg_v3b.yaml")
    conf_obj = JConfigurator(config)
    assert conf_obj.graph_config.graph("mention", "entity").index == [
        "_from",
        "_to",
        "publication@_key",
    ]
