"""平台配置。

数据目录由环境变量 MEDDEID_HOME 指定，默认 ./data。
所有持久化状态（SQLite、内容寻址 Blob、角色密钥）都位于该目录。
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_HOME = Path(os.environ.get("MEDDEID_HOME", str(ROOT / "data")))

BLOB_DIR = DATA_HOME / "blobs"
SECRETS_DIR = DATA_HOME / "secrets"
EXPORT_DIR = DATA_HOME / "exports"
DB_PATH = DATA_HOME / "platform.db"

# 演示用账户：username -> (password, roles)。生产环境应对接 IdP/MFA。
# 角色：technician（入库/队列/质检）、clinician（临床纠错、发起回链）、
#       officer（隐私官，回链第二授权人）、admin（审计导出）、
#       operator（诊断队列运维：权重/暂停/驱动调度器）、
#       observer（观察者：仅核验脱敏用量摘要）、analyst（提交/取消本团队诊断作业）。
USERS = {
    "tech":      ("tech-pw-123",      ["technician"]),
    "clinician": ("clinician-pw-123", ["clinician"]),
    "officer":   ("officer-pw-123",   ["officer"]),
    "admin":     ("admin-pw-123",     ["admin"]),
    "operator":  ("operator-pw-123",  ["operator"]),
    "observer":  ("observer-pw-123",  ["observer"]),
    "alpha":     ("alpha-pw-123",     ["analyst"]),
    "beta":      ("beta-pw-123",      ["analyst"]),
    "gamma":     ("gamma-pw-123",     ["analyst"]),
}

# 分析者归属团队（一个分析者仅属于一个团队，作业归属随登录身份冻结）。
USER_TEAM = {
    "alpha": "team-alpha",
    "beta": "team-beta",
    "gamma": "team-gamma",
}

# 任务重试上限与租约时长
MAX_ATTEMPTS = 3
JOB_LEASE_SECONDS = 600
STUDY_LOCK_STALE_SECONDS = 600

# 诊断作业租约时长；执行进程退出后租约到期，作业回到可领取（waiting）状态。
DIAG_JOB_LEASE_SECONDS = 600
# 诊断作业受理时默认等待上限（秒）：超过等待上限的等待作业终结为 wait_expired。
DIAG_WAIT_DEADLINE_SECONDS = 3600
# 诊断作业失败重试退避（秒）
DIAG_RETRY_BACKOFF_SECONDS = 0

# 回链内存保险库中分片最长存活时间
SHARE_VAULT_TTL_SECONDS = 3600


def ensure_dirs() -> None:
    for d in (DATA_HOME, BLOB_DIR, SECRETS_DIR, EXPORT_DIR):
        d.mkdir(parents=True, exist_ok=True)
