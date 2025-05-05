import dataclasses
import logging

from graphcast.architecture.edge import EdgeConfig
from graphcast.architecture.resource import Resource, ResourceHolder
from graphcast.architecture.transform import Transform
from graphcast.architecture.vertex import VertexConfig
from graphcast.onto import BaseDataclass

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SchemaMetadata(BaseDataclass):
    name: str


@dataclasses.dataclass
class Schema(BaseDataclass):
    general: SchemaMetadata
    vertex_config: VertexConfig
    edge_config: EdgeConfig
    resources: ResourceHolder
    transforms: dict[str, Transform] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        # 1. validate resources
        # 2 co-define edges from resources

        self.edge_config.finish_init(self.vertex_config)

        # modifies resources; adds extra edges found while parsing
        self.resources.finish_init(
            vc=self.vertex_config, ec=self.edge_config, transforms=self.transforms
        )

    def fetch_resource(self, name: str | None = None) -> Resource:
        _current_resource = None
        for r in self.resources.tree_likes:
            if name is None:
                _current_resource = r
            elif r.name == name:
                _current_resource = r

        for r in self.resources.row_likes:
            if name is None:
                _current_resource = r
            elif r.name == name:
                _current_resource = r
        if _current_resource is None:
            raise ValueError(f"Resource {name} not found")
        return _current_resource
