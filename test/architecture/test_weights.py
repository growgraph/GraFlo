import logging
from pathlib import Path

from graflo.architecture.actor import ActorWrapper
from graflo.architecture.onto import ActionContext
from graflo.plot.plotter import assemble_tree

logger = logging.getLogger(__name__)


def test_act_openalex(resource_openalex_authors, vc_openalex, sample_openalex_authors):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_openalex_authors)
    anw.finish_init(vertex_config=vc_openalex, transforms={})
    ctx = anw(ctx, doc=sample_openalex_authors)
    assemble_tree(anw, Path("test/figs/openalex_authors.pdf"))
    edge = ctx.acc_global[("author", "institution", None)][0]
    assert edge[-1] == {
        "updated_date": "2023-06-08",
        "created_date": "2023-06-08",
    }


def test_kg_mention(resource_kg_menton_triple, vertex_config_kg_mention, mention_data):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_kg_menton_triple)
    anw.finish_init(vertex_config=vertex_config_kg_mention, transforms={})
    ctx = anw(ctx, doc=[mention_data])
    roles = set(
        item[-1]["_role"] for item in ctx.acc_global[("mention", "mention", None)]
    )
    assert roles == {"relation", "target", "source"}
