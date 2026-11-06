import socket

import pytest

from vantage import config
from vantage.spark import SP_FILE, get_spark


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
    # keep CI green without Kafka/Azure: skip broker tests when 9092 isn't reachable,
    # and skip azure tests unless the service-principal credentials are present.
    broker = _broker_up()
    creds = SP_FILE.exists()
    skip_kafka = pytest.mark.skip(reason="no Kafka broker on localhost:9092")
    skip_azure = pytest.mark.skip(reason=f"no Azure SP credentials ({SP_FILE})")
    for item in items:
        if "kafka" in item.keywords and not broker:
            item.add_marker(skip_kafka)
        if "azure" in item.keywords and not creds:
            item.add_marker(skip_azure)
