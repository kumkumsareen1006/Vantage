import pytest

from vantage.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    # one JVM for the whole test session; only Spark tests pull this fixture
    session = get_spark(app_name="vantage-tests")
    yield session
    session.stop()
