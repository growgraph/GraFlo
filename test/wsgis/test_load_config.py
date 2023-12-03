import logging

from graph_cast.db import ConfigFactory

logger = logging.getLogger(__name__)


def test_config():
    conf = {
        "protocol": "http",
        "ip_addr": "localhost",
        "port": 333,
        "db_type": "wsgi",
        "path": "/gg",
        "paths": {"navigate": "/gg", "trends": "/trending"},
    }
    wsgi_self_obj = ConfigFactory.create_config(dict_like=conf)
    assert set(wsgi_self_obj.paths) == {"navigate", "trends"}
