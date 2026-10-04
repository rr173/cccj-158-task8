#!/usr/bin/env python3
"""队列 worker 常驻进程：循环领取并执行去标识化任务。

用法：
    MEDDEID_HOME=./data python3 scripts/worker.py [--once] [--cycles N] [--poll SECONDS]
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core import store
from app.services import diagnostics as diag_svc, queue, reidentify


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="只处理一个任务后退出")
    ap.add_argument("--cycles", type=int, default=0, help="最多处理 N 个任务后退出")
    ap.add_argument("--poll", type=float, default=1.0, help="空闲轮询间隔秒")
    args = ap.parse_args()

    store.init_db()
    diag_svc.ensure_defaults("worker")
    done = 0
    while True:
        # 既有去标识派生队列与诊断多租户队列轮流各取一个
        result = queue.run_once()
        source = "derive"
        if result is None:
            result = diag_svc.run_once("worker-loop")
            source = "diag"
        if result is None:
            # 空闲时顺带清扫过期回链授权与超等待上限的诊断作业
            reidentify.sweep_expired("worker-sweep")
            diag_svc.sweep_wait_expired("worker-sweep")
            if args.once or (args.cycles and done >= args.cycles):
                break
            time.sleep(args.poll)
            continue
        print(f"[{time.strftime('%H:%M:%S')}] {source}: {result}", flush=True)
        done += 1
        if args.once or (args.cycles and done >= args.cycles):
            break
    print(f"worker finished, processed={done}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
