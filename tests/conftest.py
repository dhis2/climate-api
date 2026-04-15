import pytest
from fastapi.testclient import TestClient

from climate_api.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)
