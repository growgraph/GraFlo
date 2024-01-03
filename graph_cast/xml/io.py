import gzip
import logging
import xml.etree.ElementTree as et
from contextlib import contextmanager
from shutil import copyfileobj

import xmltodict

from graph_cast.util.chunker import ChunkFlusherMono, FPSmart

logger = logging.getLogger(__name__)


@contextmanager
def nullcontext(enter_result=None):
    yield enter_result


def gunzip_file(fname_in, fname_out):
    with gzip.open(fname_in, "rb") as f_in:
        with open(fname_out, "wb") as f_out:
            copyfileobj(f_in, f_out)


def parse_simple(fp, good_cf):
    """
    driver func, parse file fp, push good and bad records
    accordingly to good_cf and bad_cf

    :param fp: filepointer to be parsed
    :param good_cf: chunk flusher of good records
    :return:
    """
    events = ("start", "end")
    tree = et.iterparse(fp, events)
    context = iter(tree)
    event, root = next(context)
    rec_ = "REC"
    for event, pub in context:
        if event == "end" and pub.tag == rec_:
            item = et.tostring(pub, encoding="utf8", method="xml").decode(
                "utf"
            )
            obj = xmltodict.parse(
                item,
                force_cdata=True,
                force_list=(
                    "abstract",
                    "address_name",
                    "book_note",
                    "conf_date",
                    "conf_info",
                    "conf_location",
                    "conf_title",
                    "conference",
                    "contributor",
                    "doctype",
                    "grant",
                    "grant_id",
                    "heading",
                    "identifier",
                    "keyword",
                    "language",
                    "name",
                    "organization",
                    "p",
                    "publisher",
                    "reference",
                    "rw_author",
                    "sponsor",
                    "subheading",
                    "subject",
                    "suborganization",
                    "title",
                    "edition",
                    "zip",
                ),
            )
            good_cf.push(obj)
            root.clear()
            if good_cf.stop():
                break


def convert(
    source,
    target,
    chunksize=10000,
    maxchunks=None,
    pattern=r"xmlns=\".*[^\"]\"(?=>)",
    how="standard",
):
    logger.info(f" chunksize : {chunksize} | maxchunks {maxchunks} ")

    target_prefix = target.split(".")[0]
    good_cf = ChunkFlusherMono(target_prefix, chunksize, maxchunks)
    if how == "standard":
        bad_cf = ChunkFlusherMono(
            target_prefix, chunksize, maxchunks, suffix="bad"
        )

    if isinstance(source, str):
        if source[-2:] == "gz":
            open_foo = gzip.GzipFile
        elif source[-3:] == "xml":
            open_foo = open
        else:
            raise ValueError("Unknown file type")

    with (
        open_foo(source, "rb") if isinstance(source, str) else nullcontext()
    ) as fp:
        if pattern:
            fp = FPSmart(fp, pattern)
        else:
            fp = fp
        if how == "simple":
            parse_simple(fp, good_cf)

        # terminal flush
        good_cf.flush_chunk()

        logger.error(
            f" not an error : {good_cf.items_processed()} good records"
        )
        if how == "standard":
            bad_cf.flush_chunk()
            logger.error(f"{bad_cf.items_processed()} bad records")
