#!/usr/bin/env python3
"""多租户诊断作业 worker：按加权轮转领取并执行。

用法：
    MEDDEID_HOME=./data python3 scripts/diagnostic_worker.py [--once] [--cycles N] [--poll SECONDS]

进程异常退出后，租约到期的未封存作业回到可领取状态；已封存结果的作业由后续领取
直接完成结算，不会重新计算或重复计费。
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core import store
from app.services import diagnostics


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="只处理一个作业后退出")
    ap.add_argument("--cycles", type=int, default=0, help="最多处理 N 个作业后退出")
    ap.add_argument("--poll", type=float, default=1.0, help="空闲轮询间隔秒")
    ap.add_argument("--lease", type=int, default=600, help="租约秒数")
    args = ap.parse_args()

    store.init_db()
    worker_id = f"diag-worker-{os.getpid()}"
    done = 0
    while True:
        result = diagnostics.run_once(worker_id, lease_seconds=args.lease)
        if result is None:
            if args.once or (args.cycles and done >= args.cycles):
                break
            time.sleep(args.poll)
            continue
        print(f"[{time.strftime('%H:%M:%S')}] {result}", flush=True)
        done += 1
        if args.once or (args.cycles and done >= args.cycles):
            break
    print(f"diagnostic worker finished, processed={done}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
