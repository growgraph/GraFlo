import os
from pathlib import Path

from graph_cast.architecture.onto import EncodingType
from graph_cast.util.chunker import (
    ChunkerFactory,
    FileChunker,
    JsonChunker,
    JsonlChunker,
    TableChunker,
    TrivialChunker,
)


def test_trivial():
    array = [{"a": v} for v in range(9)]
    ch = TrivialChunker(batch_size=5, array=array)
    for _ in ch:
        pass
    assert ch.cnt == 9


def test_file_chunker():
    filename = Path(
        os.path.join(os.path.dirname(__file__), "../data/csv/ticker/ticker.csv.gz")
    )
    ch = FileChunker(batch_size=5, limit=6, filename=filename)
    for _ in ch:
        pass
    assert ch.cnt == 6
    ch = FileChunker(batch_size=90, limit=250, filename=filename)
    for _ in ch:
        pass
    assert ch.cnt == 200


def test_table_chunker():
    filename = Path(
        os.path.join(os.path.dirname(__file__), "../data/csv/ticker/ticker.csv.gz")
    )
    ch = TableChunker(batch_size=5, limit=6, filename=filename)
    for item in ch:
        assert set(item[0].keys()) == set(ch.header)
    assert ch.cnt == 6


def test_jsonl_chunker():
    filename = Path(
        os.path.join(os.path.dirname(__file__), "../data/jsonl/e00000000.jsonl.gz")
    )
    ch = JsonlChunker(batch_size=5, limit=6, filename=filename)
    for item in ch:
        assert isinstance(item[0], dict)


def test_json_chunker():
    filename = Path(
        os.path.join(os.path.dirname(__file__), "../data/json/wos/wos.json.gz")
    )
    ch = JsonChunker(batch_size=5, limit=6, filename=filename)
    for item in ch:
        assert isinstance(item[0], dict)
    assert ch.cnt == 6


def test_factory():
    args = {
        "limit": 200,
        "batch_size": 50,
        "resource": Path("smth.csv.gz"),
        "encoding": EncodingType.UTF_8,
    }
    ch = ChunkerFactory.create_chunker(**args)
    assert isinstance(ch, TableChunker)
    args = {"limit": 200, "batch_size": 50, "filename": Path("smth.jcsv")}
    try:
        ch = ChunkerFactory.create_chunker(**args)
    except ValueError as e:
        assert "type" in f"{e}"
