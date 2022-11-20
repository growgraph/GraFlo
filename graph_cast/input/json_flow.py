import logging
from typing import List

from graph_cast.architecture import JConfigurator
from graph_cast.db import ConnectionConfigType, ConnectionManager
from graph_cast.db.arango.util import insert_edges_batch, upsert_docs_batch
from graph_cast.input.json import jsonlike_to_collections
from graph_cast.util import timer as timer
from graph_cast.util.transform import merge_doc_basis

logger = logging.getLogger(__name__)


def process_jsonlike(
    json_data: List,
    conf_obj: JConfigurator,
    db_config: ConnectionConfigType,
    ncores=1,
    dry=False,
):
    vdocs, edocs = jsonlike_to_collections(json_data, conf_obj, ncores)
    with timer.Timer() as t_ingest:
        cnt = 0
        with ConnectionManager(connection_config=db_config) as db_client:
            for k, v in vdocs.items():
                r = merge_doc_basis(v, conf_obj.vertex_config.index(k))
                cnt += len(r)
                query0 = upsert_docs_batch(
                    v,
                    conf_obj.vertex_config.vertex_dbname(k),
                    conf_obj.vertex_config.index(k),
                    "doc",
                    True,
                )
                if not dry:
                    db_client.execute(query0)

    logger.info(f" ingested {cnt} vertices {t_ingest.elapsed:.2f} sec")

    with timer.Timer() as t_ingest_edges:
        cnt = 0
        with ConnectionManager(connection_config=db_config) as db_client:
            for (vfrom, vto), batch in edocs.items():
                cnt += len(batch)
                query0 = insert_edges_batch(
                    batch,
                    conf_obj.vertex_config.vertex_dbname(vfrom),
                    conf_obj.vertex_config.vertex_dbname(vto),
                    conf_obj.graph(vfrom, vto).edge_name,
                    conf_obj.vertex_config.index(vfrom).fields,
                    conf_obj.vertex_config.index(vto).fields,
                    False,
                )

                if not dry:
                    db_client.execute(query0)

    logger.info(f" ingested {cnt} edges {t_ingest_edges.elapsed:.2f} sec")

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    # for uv, item in graphs.items():
    #     if item["type"] == "indirect":
    #         query0 = define_extra_edges(item)
    #         cursor = sys_db.aql.execute(query0)
