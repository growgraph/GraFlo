import argparse
import logging

import pandas as pd

from graph_cast.architecture import TConfigurator
from graph_cast.db import ConfigFactory, ConnectionManager
from graph_cast.db.connection import init_db
from graph_cast.input.table_flow import process_table
from graph_cast.util import ResourceHandler

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--clean-start", action="store_true", help="wipe all the collections"
    )

    parser.add_argument(
        "--config-path",
        type=str,
        default="conf/table/ibes.yaml",
        help="",
    )

    db_args = {
        "protocol": "http",
        "ip_addr": "127.0.0.1",
        "port": 8529,
        "cred_name": "test",
        "cred_pass": "123",
        "database": "testdb",
        "db_type": "arango",
    }

    args = parser.parse_args()

    db_args["database"] = "testdb"
    conn_conf = ConfigFactory.create_config(args=db_args)

    schema_config = ResourceHandler.load(fpath=args.config_path)
    schema_conf = TConfigurator(schema_config)
    df = pd.DataFrame(
        [
            [
                "0000",
                "87482X10",
                "TALMER BANCORP",
                "TLMR",
                "20140310",
                "RBCDOMIN",
                "ARFSTROM      J",
                "2",
                "OUTPERFORM",
                2,
                "BUY",
                659,
                71182,
                1,
                "8:54:03",
                "20160126",
                "9:35:52",
                "20140310",
                "0:20:00",
            ],
            [
                "0000",
                "87482X10",
                "TALMER BANCORP",
                "TLMR",
                "20140311",
                "JPMORGAN",
                "ALEXOPOULOS   S",
                "2",
                "OVERWEIGHT",
                2,
                "BUY",
                1243,
                79092,
                1,
                "17:10:47",
                "20160126",
                "10:09:34",
                "20140310",
                "0:25:00",
            ],
            [
                "0000",
                "87482X10",
                "TALMER BANCORP",
                "TLMR",
                "20140311",
                "KEEFE",
                "MCGRATTY      C",
                "2",
                "OUTPERFORM",
                2,
                "BUY",
                1308,
                119962,
                1,
                "15:17:15",
                "20150730",
                "7:25:58",
                "20140309",
                "17:05:00",
            ],
        ],
        columns=[
            "TICKER",
            "CUSIP",
            "CNAME",
            "OFTIC",
            "ACTDATS",
            "ESTIMID",
            "ANALYST",
            "ERECCD",
            "ETEXT",
            "IRECCD",
            "ITEXT",
            "EMASKCD",
            "AMASKCD",
            "USFIRM",
            "ACTTIMS",
            "REVDATS",
            "REVTIMS",
            "ANNDATS",
            "ANNTIMS",
        ],
    )

    schema_conf.set_mode("ibes")
    with ConnectionManager(connection_config=conn_conf) as db_client:
        init_db(db_client, schema_conf, clean_start=True)
    process_table(tabular_resource=df, conf=schema_conf, db_config=conn_conf)
