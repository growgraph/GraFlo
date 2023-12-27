import pandas as pd
from suthing import DBConnectionConfig

from graph_cast.architecture.schema import Schema
from graph_cast.db import ConnectionManager
from graph_cast.input.util import list_docs_to_graph_container
from graph_cast.onto import ResourceType


class Caster:
    def __init__(self, schema: Schema):
        self.schema = schema

    def cast(self, resource, columns=None, resource_name=None):
        vc = self.schema.vertex_config
        ec = self.schema.edge_config
        rr = self.schema.fetch_resource(resource_name)
        if rr.resource_type == ResourceType.ROWLIKE:
            rows, columns = self._normalize_resource(resource, columns)
            rr.prepare_apply(columns=columns, vertex_config=vc)
            docs = rr.apply(
                rows, vertex_config=vc, edge_config=ec, columns=columns
            )
        elif rr.resource_type == ResourceType.TREELIKE:
            docs = rr.apply(resource, vertex_config=vc)
        else:
            raise ValueError(f"unknown ResourceType {rr.resource_type}")

        graph = list_docs_to_graph_container(docs)
        return graph

    @staticmethod
    def _normalize_resource(
        data: pd.DataFrame | list[list], columns=None
    ) -> tuple[list[dict], list[str]]:
        if isinstance(data, pd.DataFrame):
            columns = data.columns.tolist()
            _data = data.values.tolist()
        else:
            _data = data
            if columns is None:
                raise ValueError(f"columns should be set")
        rows_dressed = [
            {k: v for k, v in zip(columns, item)} for item in _data
        ]
        return rows_dressed, columns

    def ingest(self, conn_conf: DBConnectionConfig):
        pass
