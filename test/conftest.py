import pytest

from graph_cast.main import ingest_files
from graph_cast.util import ResourceHandler


def pytest_addoption(parser):
    parser.addoption("--reset", action="store", default=False)


@pytest.fixture
def reset(request):
    return request.config.getoption("--reset")
