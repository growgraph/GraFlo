import logging

from graph_cast.architecture.edge import Edge
from graph_cast.filter.onto import Clause, Expression
from graph_cast.onto import DBFlavor

logger = logging.getLogger(__name__)


def define_extra_edges(g: Edge):
    """
    create a query to generate edges from u to v by w :
            (u -> w -> v) -> (u -> w) and add properties of w as properties of the edge


    :param g:
    :return:
    """
    ucol, vcol, wcol = g.source, g.target, g.by
    weight = g.weight_dict
    s = f"""FOR w IN {wcol}
        LET uset = (FOR u IN 1..1 INBOUND w {ucol}_{wcol}_edges RETURN u)
        LET vset = (FOR v IN 1..1 INBOUND w {vcol}_{wcol}_edges RETURN v)
        FOR u in uset
        FOR v in vset
    """
    s_ins_ = ", ".join([f"{v}: w.{k}" for k, v in weight.items()])
    s_ins_ = f"_from: u._id, _to: v._id, {s_ins_}"
    s_ins = f"          INSERT {{{s_ins_}}} "
    s_last = f"IN {ucol}_{vcol}_edges"
    query0 = s + s_ins + s_last
    return query0


def render_filters(filters: None | list | dict | Clause = None, doc_name="d") -> str:
    if filters is not None:
        if not isinstance(filters, Clause):
            ff = Expression.from_dict(filters)
        else:
            ff = filters
        literal_condition = ff(doc_name=doc_name, kind=DBFlavor.ARANGO)
        filter_clause = f"FILTER {literal_condition}"
    else:
        filter_clause = ""

    return filter_clause
