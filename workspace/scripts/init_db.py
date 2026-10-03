#!/usr/bin/env python3
"""初始化数据库并播种默认策略。"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core import dicomutil, store
from app.services import policies as policies_svc


def main() -> None:
    store.init_db()
    try:
        policies_svc.latest_policy("project-default")
        print("default policy already exists")
    except store.NotFound:
        pol = policies_svc.register_policy(
            "project-default", dicomutil.default_policy("strict"), "system")
        print(f"seeded project-default v{pol['version']} ({pol['id']})")
    print(f"data home: {os.environ.get('MEDDEID_HOME', './data')}")


if __name__ == "__main__":
    main()
