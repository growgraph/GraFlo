import pandas as pd
from suthing import DBConnectionConfig

from graph_cast.architecture.schema import Schema
from graph_cast.db import ConnectionManager
from graph_cast.onto import ResourceType


class Caster:
    def __init__(self, schema: Schema):
        self.schema = schema

    def cast(self, resource, columns=None, resource_name=None):
        vc = self.schema.vertex_config
        ec = self.schema.edge_config
        rr = self.schema.fetch_resource(resource_name)
        if rr.resource_type == ResourceType.ROWLIKE:
            self._normalize_resource(resource, columns)
            graph = rr.apply(
                resource, vertex_config=vc, ec=ec, columns=columns
            )
        elif rr.resource_type == ResourceType.TREELIKE:
            graph = rr.apply(resource, vertex_config=vc, ec=ec)
        else:
            raise ValueError(f"unknown ResourceType {rr.resource_type}")
        return graph

    @staticmethod
    def _normalize_resource(
        data: pd.DataFrame | list[list], columns=None
    ) -> list[dict]:
        if isinstance(data, pd.DataFrame):
            columns = data.columns
            _data = data.values.tolist()
        else:
            _data = data
            if columns is None:
                raise ValueError(f"columns should be set")
        rows_dressed = [
            {k: v for k, v in zip(columns, item)} for item in _data
        ]
        return rows_dressed

    def ingest(self, conn_conf: DBConnectionConfig):
        pass
