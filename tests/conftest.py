import socket

import pytest

from vantage import config
from vantage.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    # one JVM for the whole test session; only Spark tests pull this fixture
    session = get_spark(app_name="vantage-tests")
    yield session
    session.stop()


def _broker_up():
    host, _, port = config.KAFKA_BOOTSTRAP.split(",")[0].partition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config, items):
    # keep CI green without Kafka: skip broker tests when 9092 isn't reachable
    if _broker_up():
        return
    skip = pytest.mark.skip(reason="no Kafka broker on localhost:9092")
    for item in items:
        if "kafka" in item.keywords:
            item.add_marker(skip)
