import logging
from typing import List

from graph_cast.architecture import JConfigurator
from graph_cast.db import ConnectionManager
from graph_cast.db.arango.util import define_extra_edges, fetch_fields
from graph_cast.db.onto import DBConnectionConfig
from graph_cast.input.json import jsonlike_to_collections
from graph_cast.input.util import list_to_dict_edges, list_to_dict_vertex
from graph_cast.util import timer as timer
from graph_cast.util.merge import merge_doc_basis

logger = logging.getLogger(__name__)


def process_jsonlike(
    json_data: List,
    conf_obj: JConfigurator,
    db_config: DBConnectionConfig,
    ncores=1,
    dry=False,
    **kwargs,
):
    list_defaultdicts = jsonlike_to_collections(json_data, conf_obj, ncores)

    vdocs = list_to_dict_vertex(list_defaultdicts)

    with timer.Timer() as t_ingest:
        cnt = 0
        with ConnectionManager(connection_config=db_config) as db_client:
            for k, v in vdocs.items():
                squished_vertices = merge_doc_basis(
                    v, conf_obj.vertex_config.index(k)
                )
                cnt += len(squished_vertices)
                db_client.upsert_docs_batch(
                    squished_vertices,
                    conf_obj.vertex_config.vertex_dbname(k),
                    conf_obj.vertex_config.index(k),
                    update_keys="doc",
                    filter_uniques=True,
                    dry=dry,
                )

    logger.info(f" ingested {cnt} vertices in {t_ingest.elapsed:.2f} sec")

    # currently works only on item level
    for edge in conf_obj.post_weights:
        for weight in edge.weight_vertices:
            vname = weight.name
            index_fields = conf_obj.vertex_config.index(vname)
            retrieve_fields = [f.name for f in weight.fields]
            doc_indices = [item for item in vdocs[vname]]

            if not dry:
                weights_per_item = fetch_fields(
                    db_client=db_client,
                    docs=doc_indices,
                    collection_name=conf_obj.vertex_config.vertex_dbname(
                        vname
                    ),
                    match_keys=index_fields.fields,
                    return_keys=retrieve_fields,
                )

                for j, item in enumerate(list_defaultdicts):
                    weights = weights_per_item[j]

                    for ee in item[edge.source, edge.target]:
                        weight_collection_attached = {
                            weight.cfield(k): v for k, v in weights[0].items()
                        }
                        ee.update(weight_collection_attached)

    edocs = list_to_dict_edges(list_defaultdicts)

    with timer.Timer() as t_ingest_edges:
        cnt = 0
        with ConnectionManager(connection_config=db_config) as db_client:
            for (vfrom, vto), batch in edocs.items():
                cnt += len(batch)
                with timer.Timer() as t_ingest_edges0:
                    logger.info(f" edges : {vfrom} {vto}")
                    uniq_weight_collections = [
                        vc.name
                        for vc in conf_obj.graph(vfrom, vto).weight_vertices
                    ]
                    db_client.insert_edges_batch(
                        docs_edges=batch,
                        source_class=conf_obj.vertex_config.vertex_dbname(
                            vfrom
                        ),
                        target_class=conf_obj.vertex_config.vertex_dbname(vto),
                        relation_name=conf_obj.graph(vfrom, vto).edge_name,
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
                        dry=dry,
                        **kwargs,
                    )

                logger.info(
                    f" ingested {len(batch)} edges {vfrom}-{vto}"
                    f" in {t_ingest_edges0.elapsed:.3f} sec"
                )

    logger.info(f" ingested {cnt} edges in {t_ingest_edges.elapsed:.2f} sec")

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    # for uv, item in conf_obj.graph_config.extra_edges():
    #     with ConnectionManager(connection_config=db_config) as db_client:
    #         query0 = define_extra_edges(item)
    #         cursor = db_client.aql.execute(query0)
