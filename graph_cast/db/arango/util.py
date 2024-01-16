import logging

from graph_cast.architecture.edge import Edge

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
