import logging
from test.transform.conftest import transform_it

import pytest

from graph_cast.onto import InputType

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def modes_json():
    return ["kg_v3b"]


@pytest.fixture(scope="function")
def modes_table():
    return [
        "ibes",
        # "ticker"
    ]


def test_transform(current_path, modes_json, modes_table, reset):
    for m in modes_json:
        transform_it(
            current_path=current_path,
            input_type=InputType.JSON,
            mode=m,
            reset=reset,
        )

    for m in modes_table:
        transform_it(
            current_path=current_path,
            input_type=InputType.TABLE,
            mode=m,
            reset=reset,
        )
