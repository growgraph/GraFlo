from os import listdir
from os.path import isfile, join, expanduser
import argparse
import logging
import zipfile
import yaml
import random
import string

from pathlib import Path
import shutil
from graph_cast.xml.io import convert
import graph_cast.util.timer as timer
from graph_cast.main import ingest_json_files
from graph_cast.arango.util import get_arangodb_client

logger = logging.getLogger(__name__)


def file_is(fname, patterns):
    morphemes = fname.split(".")
    l_aux = len(patterns)
    flag = all(x == y for x, y in zip(morphemes[-l_aux:], patterns))
    return flag


def fetch_proper_filenames(fpath, maxyears, maxunits):
    files = sorted([join(fpath, f) for f in listdir(fpath) if isfile(join(fpath, f))])

    if args.maxyears:
        files = files[:maxyears]

    for f in files:
        logger.info(f)

    acc = []

    for f in files:
        with zipfile.ZipFile(f, "r") as zip_ref:
            fcontent = sorted(
                [item for item in zip_ref.namelist() if file_is(item, ["xml", "gz"])]
            )
            logger.info(fcontent)
            acc += [(f, y) for y in fcontent]

    if args.maxunits:
        acc = acc[:maxunits]
    logger.info(acc)
    return acc


def create_tmp(fpath):
    Path(fpath).mkdir(parents=True, exist_ok=True)


def process_units(
    units,
    tmp_dir,
    config,
    db_client,
    init_collections,
    dry,
):
    with timer.Timer() as t_full:
        for j, (zfile, unit) in enumerate(units):
            with timer.Timer() as t:
                convert_unit(zfile, unit, tmp_dir)
            logger.info(
                f"{unit} from {zfile.split('/')[-1]} conversion to json took {t.elapsed:.2f} sec"
            )
            with timer.Timer() as t_ingest:
                ingest_json_files(
                    tmp_dir,
                    config,
                    db_client=db_client,
                    keyword="json",
                    clean_start="all" if j == 0 and init_collections else None,
                    dry=dry,
                )

            logger.info(
                f"{unit} from {zfile.split('/')[0]} ingestion to db took {t_ingest.elapsed:.2f} sec"
            )
            clean_tmp(tmp_dir)

    logger.info(f"complete processing of {len(units)} took {t_full.elapsed:.2f} sec")


def convert_unit(fpath, unit, tmp_dir):
    prefix = unit.split(".")
    tmp_fname = prefix[0] + ".json.gz"
    with zipfile.ZipFile(fpath) as z:
        z.extract(unit, tmp_dir)
    convert(
        join(tmp_dir, unit),
        join(tmp_dir, tmp_fname),
        chunksize=10000,
        maxchunks=None,
        how="simple",
    )


def clean_tmp(tmp_dir):
    shutil.rmtree(tmp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    logging.basicConfig(
        filename="ingest_pipeline.log",
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    parser.add_argument("--path", type=str, help="path to zip file")

    parser.add_argument(
        "--maxyears",
        type=int,
        nargs="?",
        const=1,
        help="max number of years to process",
    )

    parser.add_argument(
        "--maxunits",
        type=int,
        nargs="?",
        const=1,
        help="max number of units to process",
    )

    parser.add_argument(
        "--config-path",
        type=str,
        default="../conf/wos_json.yaml",
        help="",
    )

    parser.add_argument(
        "-i",
        "--id-addr",
        default="127.0.0.1",
        type=str,
        help="port for arangodb connection",
    )

    parser.add_argument(
        "--protocol", default="http", type=str, help="protocol for arangodb connection"
    )

    parser.add_argument(
        "-p", "--port", default=8529, type=int, help="port for arangodb connection"
    )

    parser.add_argument(
        "-l", "--cred-name", default="root", help="login name for arangodb connection"
    )

    parser.add_argument(
        "-w",
        "--cred-pass",
        default="123",
        help="login password for arangodb connection",
    )

    parser.add_argument("--db", default="_system", help="db for arangodb connection")

    parser.add_argument("--dry", action="store_true")

    parser.add_argument("--init-collections", action="store_true")

    args = parser.parse_args()

    logger.info(
        f" Initialize collections : {args.init_collections}; dry ingestion:  {args.dry} "
    )
    logger.info(f" max years : {args.maxyears} |  max total units : {args.maxunits}")

    with open(args.config_path, "r") as f:
        config_ = yaml.load(f, Loader=yaml.FullLoader)

    working_path = expanduser(args.path)
    units = fetch_proper_filenames(working_path, args.maxyears, args.maxunits)

    tmp_path = join(
        "/tmp",
        "tmp_" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6)),
    )
    create_tmp(tmp_path)

    sys_db = get_arangodb_client(
        args.protocol,
        args.ip_addr,
        args.port,
        args.database,
        args.cred_name,
        args.cred_pass,
    )

    process_units(
        units,
        tmp_path,
        config_,
        db_client=sys_db,
        init_collections=args.init_collections,
        dry=args.dry,
    )
