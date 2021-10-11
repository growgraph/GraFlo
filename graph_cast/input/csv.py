from itertools import product
import pandas as pd

from graph_cast.input.csv_abs import table_to_vcollections
from graph_cast.util.io import Chunker, ChunkerDataFrame

from graph_cast.arango.util import (
    upsert_docs_batch,
    insert_edges_batch,
    insert_return_batch,
)


def process_table(tabular_resource, batch_size, max_lines, db_client, conf):

    if isinstance(tabular_resource, pd.DataFrame):
        chk = ChunkerDataFrame(tabular_resource, batch_size, max_lines)
    elif isinstance(tabular_resource, str):
        chk = Chunker(tabular_resource, batch_size, max_lines, encoding=conf.encoding)
        conf.set_current_resource_name(tabular_resource)
    else:
        raise TypeError(f"tabular_resource type is not str or pd.DataFrame")
    header = chk.pop_header()
    header_dict = dict(zip(header, range(len(header))))

    while not chk.done:
        lines = chk.pop()
        if lines:
            vdocuments, edocuments = table_to_vcollections(
                lines,
                header_dict,
                conf,
            )

            # TODO move db related stuff out
            for vcol, batches in vdocuments.items():
                for j, data in enumerate(batches):
                    # blank nodes: push and get back their keys  {"_key": ...}
                    if vcol in conf.vertex_config.blank_collections:
                        query0 = insert_return_batch(
                            data, conf.vertex_config.dbname(vcol)
                        )
                        cursor = db_client.aql.execute(query0)
                        vdocuments[vcol][j] = [item for item in cursor]
                    else:
                        query0 = upsert_docs_batch(
                            data,
                            conf.vertex_config.dbname(vcol),
                            conf.vertex_config.index(vcol),
                            "doc",
                            True,
                        )
                        cursor = db_client.aql.execute(query0)

            # update edge data with blank node edges
            for vcol in conf.vertex_config.blank_collections:
                for vfrom, vto in conf.current_graphs:
                    if vcol == vfrom or vcol == vto:
                        for from_batch, to_batch in product(
                            vdocuments[vfrom], vdocuments[vto]
                        ):
                            edocuments[(vfrom, vto)].extend(
                                [
                                    {"source": x, "target": y}
                                    for x, y in zip(from_batch, to_batch)
                                ]
                            )

            for (vfrom, vto), data in edocuments.items():
                query0 = insert_edges_batch(
                    data,
                    conf.vertex_config.dbname(vfrom),
                    conf.vertex_config.dbname(vto),
                    conf.graph(vfrom, vto)["edge_name"],
                    conf.vertex_config.index(vfrom),
                    conf.vertex_config.index(vto),
                    False,
                )
                cursor = db_client.aql.execute(query0)
