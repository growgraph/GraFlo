import logging
import multiprocessing as mp
import queue
from functools import partial
from os import listdir
from os.path import isfile, join
from pathlib import Path

import pandas as pd
from suthing import DBConnectionConfig, Timer

from graph_cast.architecture.onto import SOURCE_AUX, TARGET_AUX, GraphContainer
from graph_cast.architecture.resource import Resource
from graph_cast.architecture.schema import Schema
from graph_cast.architecture.util import list_docs_to_graph_container
from graph_cast.db import ConnectionManager
from graph_cast.onto import ResourceType
from graph_cast.util.chunker import ChunkerFactory

logger = logging.getLogger(__name__)


class Caster:
    def __init__(self, schema: Schema, **kwargs):
        self.clean_start: bool = False
        self.n_cores = kwargs.pop("n_cores", 1)
        self.max_items = kwargs.pop("max_items", None)
        self.batch_size = kwargs.pop("batch_size", 10000)
        self.dry = kwargs.pop("dry", False)
        self.schema = schema

    @staticmethod
    def discover_files(
        fpath: Path, limit_files=None, pattern=None
    ) -> list[Path]:
        files = [
            Path(join(fpath, f))
            for f in listdir(fpath)
            if isfile(join(fpath, f))
            and (True if pattern is None else pattern in f)
        ]

        if limit_files is not None:
            files = files[:limit_files]

        return files

    def cast_normal_resource(
        self, resource, columns=None, resource_name=None
    ) -> GraphContainer:
        vc = self.schema.vertex_config
        ec = self.schema.edge_config
        rr = self.schema.fetch_resource(resource_name)
        if columns is None:
            columns = list(resource[0].keys())
        if rr.resource_type == ResourceType.ROWLIKE:
            rr.prepare_apply(columns=columns, vertex_config=vc)
            docs = rr.apply(
                resource, vertex_config=vc, edge_config=ec, columns=columns
            )
        elif rr.resource_type == ResourceType.TREELIKE:
            docs = rr.apply(resource, vertex_config=vc)
        else:
            raise ValueError(f"unknown ResourceType {rr.resource_type}")

        graph = list_docs_to_graph_container(docs)
        return graph

    def process_batch(
        self,
        batch,
        resource: Resource,
        conn_conf: None | DBConnectionConfig = None,
    ):
        gc = self.cast_normal_resource(batch)
        if conn_conf is not None:
            self.push_db(gc, conn_conf, resource)

    def process_resource(
        self,
        resource,
        resource_config: Resource,
        conn_conf: None | DBConnectionConfig = None,
    ):
        """

        Args:
            resource: file
            conn_conf:
            resource_config:

        Returns:

        """

        chunker = ChunkerFactory.create_chunker(
            filename=resource, batch_size=self.batch_size, limit=self.max_items
        )
        for batch in chunker:
            self.process_batch(
                batch, resource=resource_config, conn_conf=conn_conf
            )

    def push_db(
        self,
        gc: GraphContainer,
        conn_conf: DBConnectionConfig,
        resource: Resource,
    ):
        vc = self.schema.vertex_config
        with ConnectionManager(connection_config=conn_conf) as db_client:
            for vcol, data in gc.vertices.items():
                # blank nodes: push and get back their keys  {"_key": ...}
                if vcol in vc.blank_vertices:
                    query0 = db_client.insert_return_batch(
                        data, vc.vertex_dbname(vcol)
                    )
                    cursor = db_client.execute(query0)
                    gc.vertices[vcol] = [item for item in cursor]
                else:
                    db_client.upsert_docs_batch(
                        data,
                        vc.vertex_dbname(vcol),
                        vc.index(vcol),
                        update_keys="doc",
                        filter_uniques=True,
                        dry=self.dry,
                    )

            # update edge misc with blank node edges
            for vcol in vc.blank_vertices:
                for edge in self.schema.edge_config.edges:
                    vfrom, vto, relation = edge.edge_id
                    if vcol == vfrom or vcol == vto:
                        if edge.edge_id not in gc.edges:
                            gc.edges[edge.edge_id] = []
                        gc.edges[edge.edge_id].extend(
                            [
                                {SOURCE_AUX: x, TARGET_AUX: y}
                                for x, y in zip(
                                    gc.vertices[vfrom], gc.vertices[vto]
                                )
                            ]
                        )

        with ConnectionManager(connection_config=conn_conf) as db_client:
            # currently works only on item level
            for edge in resource.extra_weights:
                for weight in edge.weights.vertices:
                    assert weight.name is not None
                    index_fields = vc.index(weight.name)

                    if not self.dry:
                        weights_per_item = db_client.fetch_present_documents(
                            class_name=vc.vertex_dbname(weight.name),
                            batch=gc.vertices[weight.name],
                            match_keys=index_fields.fields,
                            keep_keys=weight.fields,
                        )

                        for j, item in enumerate(gc.linear):
                            weights = weights_per_item[j]

                            for ee in item[edge.edge_id]:
                                weight_collection_attached = {
                                    weight.cfield(k): v
                                    for k, v in weights[0].items()
                                }
                                ee.update(weight_collection_attached)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            for edge in self.schema.edge_config.edges:
                if edge.edge_id in gc.edges:
                    data = gc.edges[edge.edge_id]
                    db_client.insert_edges_batch(
                        docs_edges=data,
                        source_class=vc.vertex_dbname(edge.source),
                        target_class=vc.vertex_dbname(edge.target),
                        relation_name=edge.relation,
                        collection_name=edge.collection_name,
                        match_keys_source=vc.index(edge.source).fields,
                        match_keys_target=vc.index(edge.target).fields,
                        filter_uniques=False,
                        dry=self.dry,
                    )

    def process_with_queue(self, tasks: mp.Queue, **kwargs):
        while True:
            try:
                task = tasks.get_nowait()
                filepath, resource = task
            except queue.Empty:
                break
            else:
                self.process_resource(
                    resource=filepath, resource_config=resource, **kwargs
                )

    @staticmethod
    def normalize_resource(
        data: pd.DataFrame | list[list] | list[dict], columns=None
    ) -> list[dict]:
        if isinstance(data, pd.DataFrame):
            columns = data.columns.tolist()
            _data = data.values.tolist()
        elif isinstance(data[0], list):
            _data = data
            if columns is None:
                raise ValueError(f"columns should be set")
        else:
            return data  # type: ignore
        rows_dressed = [
            {k: v for k, v in zip(columns, item)} for item in _data
        ]
        return rows_dressed

    def ingest_files(self, path: Path, **kwargs):
        """

        Args:
            path:
            **kwargs:

        Returns:

        """
        conn_conf: DBConnectionConfig = kwargs.get("conn_conf", None)
        self.clean_start = kwargs.pop("clean_start", self.clean_start)
        self.n_cores = kwargs.pop("n_cores", self.n_cores)
        self.max_items = kwargs.pop("max_items", self.max_items)
        self.batch_size = kwargs.pop("batch_size", self.batch_size)
        limit_files = kwargs.pop("limit_files", None)
        patterns = kwargs.pop("patterns", {})

        with ConnectionManager(connection_config=conn_conf) as db_client:
            db_client.init_db(self.schema, self.clean_start)

        tasks = []
        for r in self.schema.resources:
            files = Caster.discover_files(
                path, limit_files=limit_files, pattern=r.name
            )
            logger.info(f"For resource name {r.name} {len(files)} were found")
            tasks += [(f, r) for f in files]

        with Timer() as klepsidra:
            if self.n_cores > 1:
                queue_tasks: mp.Queue = mp.Queue()
                for item in tasks:
                    queue_tasks.put(item)

                func = partial(
                    self.process_with_queue,
                    **kwargs,
                )
                assert (
                    mp.get_start_method() == "fork"
                ), "Requires 'forking' operating system"

                processes = []

                for w in range(self.n_cores):
                    p = mp.Process(
                        target=func, args=(queue_tasks,), kwargs=kwargs
                    )
                    processes.append(p)
                    p.start()
                    for p in processes:
                        p.join()
            else:
                for f, r in tasks:
                    self.process_resource(
                        resource=f, resource_config=r, **kwargs
                    )
        logger.info(f"Processing took {klepsidra.elapsed:.1f} sec")
