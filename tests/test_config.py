from pathlib import Path
import runpy

from src.config import settings


def test_api_server_port_is_fixed_to_8000():
    assert settings.app_host == "127.0.0.1"
    assert settings.app_port == 8000


def test_run_api_script_loads_fixed_server_config():
    namespace = runpy.run_path(str(Path("scripts/run_api.py")), run_name="run_api_test")

    assert namespace["settings"].app_host == "127.0.0.1"
    assert namespace["settings"].app_port == 8000
