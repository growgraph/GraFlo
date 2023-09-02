import pytest


def pytest_addoption(parser):
    parser.addoption("--reset", action="store", default=False)


@pytest.fixture
def reset(request):
    return request.config.getoption("--reset")
