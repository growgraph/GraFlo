"""Data casting and ingestion system for graph databases.

This module provides functionality for casting and ingesting data into graph databases.
It handles batch processing, file discovery, and database operations for both ArangoDB
and Neo4j.

Key Components:
    - Caster: Main class for data casting and ingestion
    - FilePattern: Pattern matching for file discovery
    - Patterns: Collection of file patterns for different resources

Example:
    >>> caster = Caster(schema=schema)
    >>> caster.ingest_files(path="data/", conn_conf=db_config)
"""

import logging
import multiprocessing as mp
import queue
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import pandas as pd
from suthing import ConnectionKind, DBConnectionConfig, Timer

from graphcast.architecture.onto import SOURCE_AUX, TARGET_AUX, GraphContainer
from graphcast.architecture.schema import Schema
from graphcast.db import ConnectionManager
from graphcast.util.chunker import ChunkerFactory
from graphcast.util.onto import FilePattern, Patterns

logger = logging.getLogger(__name__)


class Caster:
    """Main class for data casting and ingestion.

    This class handles the process of casting data into graph structures and
    ingesting them into the database. It supports batch processing, parallel
    execution, and various data formats.

    Attributes:
        clean_start: Whether to clean the database before ingestion
        n_cores: Number of CPU cores to use for parallel processing
        max_items: Maximum number of items to process
        batch_size: Size of batches for processing
        n_threads: Number of threads for parallel processing
        dry: Whether to perform a dry run (no database changes)
        schema: Schema configuration for the graph
    """

    def __init__(self, schema: Schema, **kwargs):
        """Initialize the caster with schema and configuration.

        Args:
            schema: Schema configuration for the graph
            **kwargs: Additional configuration options:
                - clean_start: Whether to clean the database before ingestion
                - n_cores: Number of CPU cores to use
                - max_items: Maximum number of items to process
                - batch_size: Size of batches for processing
                - n_threads: Number of threads for parallel processing
                - dry: Whether to perform a dry run
        """
        self.clean_start: bool = False
        self.n_cores = kwargs.pop("n_cores", 1)
        self.max_items = kwargs.pop("max_items", None)
        self.batch_size = kwargs.pop("batch_size", 10000)
        self.n_threads = kwargs.pop("n_threads", 1)
        self.dry = kwargs.pop("dry", False)
        self.schema = schema

    @staticmethod
    def discover_files(
        fpath: Path | str, pattern: FilePattern, limit_files=None
    ) -> list[Path]:
        """Discover files matching a pattern in a directory.

        Args:
            fpath: Path to search in
            pattern: Pattern to match files against
            limit_files: Optional limit on number of files to return

        Returns:
            list[Path]: List of matching file paths

        Raises:
            AssertionError: If pattern.sub_path is None
        """
        assert pattern.sub_path is not None
        if isinstance(fpath, str):
            fpath_pathlib = Path(fpath)
        else:
            fpath_pathlib = fpath

        files = [
            f
            for f in (fpath_pathlib / pattern.sub_path).iterdir()
            if f.is_file()
            and (
                True
                if pattern.regex is None
                else re.search(pattern.regex, f.name) is not None
            )
        ]

        if limit_files is not None:
            files = files[:limit_files]

        return files

    def cast_normal_resource(
        self, data, resource_name: str | None = None
    ) -> GraphContainer:
        """Cast data into a graph container using a resource.

        Args:
            data: Data to cast
            resource_name: Optional name of the resource to use

        Returns:
            GraphContainer: Container with cast graph data
        """
        rr = self.schema.fetch_resource(resource_name)

        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            docs = list(
                executor.map(
                    lambda doc: rr(doc),
                    data,
                )
            )

        graph = GraphContainer.from_docs_list(docs)
        return graph

    def process_batch(
        self,
        batch,
        resource_name: str | None,
        conn_conf: None | DBConnectionConfig = None,
    ):
        """Process a batch of data.

        Args:
            batch: Batch of data to process
            resource_name: Optional name of the resource to use
            conn_conf: Optional database connection configuration
        """
        gc = self.cast_normal_resource(batch, resource_name=resource_name)

        if conn_conf is not None:
            self.push_db(gc, conn_conf, resource_name=resource_name)

    def process_resource(
        self,
        resource_instance: Path,
        resource_name: str | None,
        conn_conf: None | DBConnectionConfig = None,
        **kwargs,
    ):
        """Process a resource instance.

        Args:
            resource_instance: Path to the resource file
            resource_name: Optional name of the resource
            conn_conf: Optional database connection configuration
        """
        chunker = ChunkerFactory.create_chunker(
            resource=resource_instance,
            batch_size=self.batch_size,
            limit=self.max_items,
            **kwargs,
        )
        for batch in chunker:
            self.process_batch(batch, resource_name=resource_name, conn_conf=conn_conf)

    def push_db(
        self,
        gc: GraphContainer,
        conn_conf: DBConnectionConfig,
        resource_name: str | None,
    ):
        """Push graph container data to the database.

        Args:
            gc: Graph container with data to push
            conn_conf: Database connection configuration
            resource_name: Optional name of the resource
        """
        vc = self.schema.vertex_config
        resource = self.schema.fetch_resource(resource_name)
        with ConnectionManager(connection_config=conn_conf) as db_client:
            for vcol, data in gc.vertices.items():
                # blank nodes: push and get back their keys  {"_key": ...}
                if vcol in vc.blank_vertices:
                    query0 = db_client.insert_return_batch(data, vc.vertex_dbname(vcol))
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
                for edge_id, edge in self.schema.edge_config.edges_items():
                    vfrom, vto, relation = edge_id
                    if vcol == vfrom or vcol == vto:
                        if edge_id not in gc.edges:
                            gc.edges[edge_id] = []
                        gc.edges[edge_id].extend(
                            [
                                {SOURCE_AUX: x, TARGET_AUX: y}
                                for x, y in zip(gc.vertices[vfrom], gc.vertices[vto])
                            ]
                        )

        with ConnectionManager(connection_config=conn_conf) as db_client:
            # currently works only on item level
            for edge in resource.extra_weights:
                if edge.weights is None:
                    continue
                for weight in edge.weights.vertices:
                    assert weight.name is not None
                    index_fields = vc.index(weight.name)

                    if not self.dry and weight.name in gc.vertices:
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
                                    weight.cfield(k): v for k, v in weights[0].items()
                                }
                                ee.update(weight_collection_attached)

        with ConnectionManager(connection_config=conn_conf) as db_client:
            for edge_id, edge in self.schema.edge_config.edges_items():
                for ee in gc.loop_over_relations(edge_id):
                    _, _, relation = ee
                    if not self.dry:
                        data = gc.edges[ee]
                        db_client.insert_edges_batch(
                            docs_edges=data,
                            source_class=vc.vertex_dbname(edge.source),
                            target_class=vc.vertex_dbname(edge.target),
                            relation_name=relation,
                            collection_name=edge.collection_name,
                            match_keys_source=vc.index(edge.source).fields,
                            match_keys_target=vc.index(edge.target).fields,
                            filter_uniques=False,
                            dry=self.dry,
                        )

    def process_with_queue(self, tasks: mp.Queue, **kwargs):
        """Process tasks from a queue.

        Args:
            tasks: Queue of tasks to process
            **kwargs: Additional keyword arguments
        """
        while True:
            try:
                task = tasks.get_nowait()
                filepath, resource_name = task
            except queue.Empty:
                break
            else:
                self.process_resource(
                    resource_instance=filepath, resource_name=resource_name, **kwargs
                )

    @staticmethod
    def normalize_resource(
        data: pd.DataFrame | list[list] | list[dict], columns=None
    ) -> list[dict]:
        """Normalize resource data into a list of dictionaries.

        Args:
            data: Data to normalize (DataFrame, list of lists, or list of dicts)
            columns: Optional column names for list data

        Returns:
            list[dict]: Normalized data as list of dictionaries

        Raises:
            ValueError: If columns is not provided for list data
        """
        if isinstance(data, pd.DataFrame):
            columns = data.columns.tolist()
            _data = data.values.tolist()
        elif isinstance(data[0], list):
            _data = data
            if columns is None:
                raise ValueError("columns should be set")
        else:
            return data  # type: ignore
        rows_dressed = [{k: v for k, v in zip(columns, item)} for item in _data]
        return rows_dressed

    def ingest_files(self, path: Path | str, **kwargs):
        """Ingest files from a directory.

        Args:
            path: Path to directory containing files
            **kwargs: Additional keyword arguments:
                - conn_conf: Database connection configuration
                - clean_start: Whether to clean the database before ingestion
                - n_cores: Number of CPU cores to use
                - max_items: Maximum number of items to process
                - batch_size: Size of batches for processing
                - dry: Whether to perform a dry run
                - init_only: Whether to only initialize the database
                - limit_files: Optional limit on number of files to process
                - patterns: Optional file patterns to match
        """
        conn_conf: DBConnectionConfig = kwargs.get("conn_conf")
        self.clean_start = kwargs.pop("clean_start", self.clean_start)
        self.n_cores = kwargs.pop("n_cores", self.n_cores)
        self.max_items = kwargs.pop("max_items", self.max_items)
        self.batch_size = kwargs.pop("batch_size", self.batch_size)
        self.dry = kwargs.pop("dry", self.dry)
        init_only = kwargs.pop("init_only", False)
        limit_files = kwargs.pop("limit_files", None)
        patterns = kwargs.pop("patterns", Patterns())

        if (
            conn_conf.connection_type == ConnectionKind.ARANGO
            and conn_conf.database == "_system"
        ):
            db_name = self.schema.general.name
            try:
                with ConnectionManager(connection_config=conn_conf) as db_client:
                    db_client.create_database(db_name)
            except Exception as exc:
                logger.error(exc)

            conn_conf.database = db_name

        with ConnectionManager(connection_config=conn_conf) as db_client:
            db_client.init_db(self.schema, self.clean_start)

        if init_only:
            logger.info("ingest execution bound to init")
            sys.exit(0)

        tasks: list[tuple[Path, str]] = []
        for r in self.schema.resources:
            pattern = (
                FilePattern(regex=r.name)
                if r.name not in patterns.patterns
                else patterns.patterns[r.name]
            )
            files = Caster.discover_files(
                path, limit_files=limit_files, pattern=pattern
            )
            logger.info(f"For resource name {r.name} {len(files)} were found")
            tasks += [(f, r.name) for f in files]

        with Timer() as klepsidra:
            if self.n_cores > 1:
                queue_tasks: mp.Queue = mp.Queue()
                for item in tasks:
                    queue_tasks.put(item)

                func = partial(
                    self.process_with_queue,
                    **kwargs,
                )
                assert mp.get_start_method() == "fork", (
                    "Requires 'forking' operating system"
                )

                processes = []

                for w in range(self.n_cores):
                    p = mp.Process(target=func, args=(queue_tasks,), kwargs=kwargs)
                    processes.append(p)
                    p.start()
                    for p in processes:
                        p.join()
            else:
                for f, resource_name in tasks:
                    self.process_resource(
                        resource_instance=f, resource_name=resource_name, **kwargs
                    )
        logger.info(f"Processing took {klepsidra.elapsed:.1f} sec")
