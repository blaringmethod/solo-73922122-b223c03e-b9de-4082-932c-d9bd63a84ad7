import atexit
import shutil
import sys
import tempfile
from pathlib import Path

# Make the project root importable when running pytest from the repo root.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Keep test artifacts out of the default location used by a local uvicorn.
import app.config as _config  # noqa: E402

_TEST_RESULT_DIR = tempfile.mkdtemp(prefix="sql-sandbox-test-results-")
_config.RESULT_DIR = _TEST_RESULT_DIR
atexit.register(shutil.rmtree, _TEST_RESULT_DIR, True)
