"""Early-boot side effects.

This module is imported before any other eo_api modules so that
environment variables and logging are configured before other imports.
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv  # noqa: E402

# -- Load .env (must happen before pygeoapi reads PYGEOAPI_CONFIG) ------------
load_dotenv()

DEFAULT_PYGEOAPI_CONFIG = Path(__file__).resolve().parent.parent.parent / "data" / "pygeoapi" / "pygeoapi-config.yml"
DEFAULT_PYGEOAPI_OPENAPI = Path(__file__).resolve().parent.parent.parent / "data" / "pygeoapi" / "pygeoapi-openapi.yml"
os.environ.setdefault("PYGEOAPI_CONFIG", str(DEFAULT_PYGEOAPI_CONFIG))
os.environ.setdefault("PYGEOAPI_OPENAPI", str(DEFAULT_PYGEOAPI_OPENAPI))

# -- eo_api / third-party logging setup ---------------------------------------
eo_logger = logging.getLogger("eo_api")
eo_logger.setLevel(logging.INFO)
if not eo_logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    eo_logger.addHandler(handler)
eo_logger.propagate = False
