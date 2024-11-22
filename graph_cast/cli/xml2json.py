import logging
import pathlib
import sys

import click

from graph_cast.util.chunker import convert, force_list_wos, tag_wos

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-s",
    "--source-path",
    type=click.Path(path_type=pathlib.Path),
    required=True,
)
@click.option("-c", "--chunk-size", type=int, default=1000)
@click.option("-m", "--max-chunks", type=int, default=None)
@click.option("--mode", type=str)
def do(source_path, chunk_size, max_chunks, mode):
    if mode == "wos":
        pattern = r"xmlns=\".*[^\"]\"(?=>)"
        force_list = force_list_wos
        tag = tag_wos
    elif mode == "pubmed":
        pattern = None
        force_list = None
        tag = "PubmedArticle"
    else:
        raise ValueError(f"Unknown mode {mode}")

    if source_path.is_dir():
        files = [
            fp for fp in source_path.iterdir() if not fp.is_dir() and "xml" in fp.name
        ]
    else:
        files = [source_path] if ".xml." in source_path.name else []
    for fp in files:
        target_root = str(fp.parent / fp.name.split(".")[0])

        convert(
            fp,
            target_root=target_root,
            chunk_size=chunk_size,
            max_chunks=max_chunks,
            pattern=pattern,
            force_list=force_list,
            root_tag=tag,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    do()
