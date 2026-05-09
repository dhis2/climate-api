import os
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from climate_api import config as api_config
from climate_api.main import app

_TEST_CONFIG = """\
extent:
  id: sle
  name: Sierra Leone
  bbox: [-13.5, 6.9, -10.1, 10.0]
  country_code: SLE
data_dir: ./data
"""


@pytest.fixture(autouse=True, scope="session")
def _test_climate_api_config(tmp_path_factory: pytest.TempPathFactory) -> Generator[None, None, None]:
    config_file = tmp_path_factory.mktemp("config") / "climate-api.yaml"
    config_file.write_text(_TEST_CONFIG, encoding="utf-8")
    old = os.environ.get("CLIMATE_API_CONFIG")
    os.environ["CLIMATE_API_CONFIG"] = str(config_file)
    yield
    if old is None:
        os.environ.pop("CLIMATE_API_CONFIG", None)
    else:
        os.environ["CLIMATE_API_CONFIG"] = old


@pytest.fixture(autouse=True)
def _reset_config_cache() -> Generator[None, None, None]:
    api_config._cache = None
    yield
    api_config._cache = None


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)
