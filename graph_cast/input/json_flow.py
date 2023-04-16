import logging
from typing import List

from graph_cast.architecture import JConfigurator
from graph_cast.db import ConnectionConfigType, ConnectionManager
from graph_cast.db.arango.util import (
    define_extra_edges,
    insert_edges_batch,
    upsert_docs_batch,
)
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
    **kwargs,
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
                with timer.Timer() as t_ingest_edges0:
                    logger.info(f" edges : {vfrom} {vto}")
                    uniq_weight_collections = [
                        vc.collection_name
                        for vc in conf_obj.graph(vfrom, vto).weight_vertices
                    ]
                    query0 = insert_edges_batch(
                        docs_edges=batch,
                        source_collection_name=conf_obj.vertex_config.vertex_dbname(
                            vfrom
                        ),
                        target_collection_name=conf_obj.vertex_config.vertex_dbname(
                            vto
                        ),
                        edge_col_name=conf_obj.graph(vfrom, vto).edge_name,
                        match_keys_source=conf_obj.vertex_config.index(
                            vfrom
                        ).fields,
                        match_keys_target=conf_obj.vertex_config.index(
                            vto
                        ).fields,
                        filter_uniques=False,
                        uniq_weight_collections=uniq_weight_collections,
                        uniq_weight_fields=conf_obj.graph(
                            vfrom, vto
                        ).weight_fields,
                        **kwargs,
                    )
                    if not dry:
                        db_client.execute(query0)
                logger.info(
                    f" ingested {len(batch)} edges {vfrom}-{vto}"
                    f" {t_ingest_edges0.elapsed:.3f} sec"
                )

    logger.info(f" ingested {cnt} edges {t_ingest_edges.elapsed:.2f} sec")

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    # for uv, item in conf_obj.graph_config.extra_edges():
    #     with ConnectionManager(connection_config=db_config) as db_client:
    #         query0 = define_extra_edges(item)
    #         cursor = db_client.aql.execute(query0)
