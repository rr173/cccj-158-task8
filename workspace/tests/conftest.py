"""测试夹具：每个测试独立的临时数据目录与全新加载的 app 包。"""
import importlib
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture()
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("MEDDEID_HOME", str(tmp_path))
    # 全部 app 模块重新导入，使 config/store 的线程内状态指向本测试目录
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    importlib.invalidate_caches()

    from app import config
    from app.core import store, dicomutil
    from app.services import policies as policies_svc, vault as vault_mod
    store.init_db(reset=True)
    policies_svc.register_policy(
        "project-default", dicomutil.default_policy("strict"), "system")

    yield tmp_path

    try:
        store.get_conn().close()
    except Exception:
        pass
    vault_mod.vault._data.clear()
