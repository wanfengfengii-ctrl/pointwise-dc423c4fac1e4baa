"""真实进程中断核验：用独立子进程模拟崩溃，另一个子进程续办。

阶段进程 1：建库、泵入至 [a]（含未提交写入）后以 os._exit(99) 硬退出；
阶段进程 2：以 None 重开两库，断言重放确认、续泵、撤子域、换源结果全部正确。
任何断言失败均以非零退出码传播。
"""

from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from capture import C, S  # noqa: E402

SEED = {"d": {"x": 0, "y": 0, "note": None}}


def stage_crash(work: str) -> None:
    events = [
        ["b", "t1"],
        ["s", "t2", "u1", "/t1"],
        ["w", "t3", "", "d", {"x": 2}],
        ["w", "t4", "/t1/u1", "d", {"y": 90}],
        ["r", "t5", "/t1/u1"],                          # 孙域 y=90 随撤回失效
        ["a"],                                          # 崩溃前确认到 6
        ["b", "t6"],
        ["w", "t7", "/t6", "d", {"y": 5}],
        ["r", "t8", "/t6"],                             # 重开后撤回父域
        ["a"],
    ]
    s = S(os.path.join(work, "src.db"), events, sid="A")
    c = C(os.path.join(work, "cap.db"), SEED)
    assert c.pump(s, n=6) == 6
    c.read()
    s.close()
    c.close()
    # 硬退出：不执行任何 Python 层清理，模拟进程被 kill
    os._exit(99)


def stage_resume(work: str) -> None:
    s = S(os.path.join(work, "src.db"), None)
    c = C(os.path.join(work, "cap.db"), None)

    # 重开先补确认（源库其实已收到 ack；重放必须幂等），n=0 亦然
    seen = []
    assert c.pump(s, n=0, h=lambda p, i, e: seen.append((p, i))) == 0
    assert seen == [(2, 6)]
    assert s.acked == 6

    assert c.pump(s, n=None) == 4
    assert s.acked == 10
    assert s.next(10) is None
    consumed, ackreq, rows, receipts = c.read()
    assert (consumed, ackreq) == (10, 10)
    assert rows == [["d", [2, 0, None]]], rows
    assert receipts == []

    # 撤父域后换源 B 提交
    s2 = S(os.path.join(work, "src_b.db"), [
        ["b", "b1"],
        ["w", "b2", "/b1", "d", {"y": 3, "note": "B"}],
        ["c", "b3"],
        ["a"],
    ], sid="B")
    assert c.pump(s2, n=None) == 4
    _, _, rows, receipts = c.read()
    assert rows == [["d", [2, 3, "B"]]], rows
    assert receipts == [["b3", 3, [["d", [2, 3, "B"]]]]]
    s.close()
    s2.close()
    c.close()
    print("crash-resume verification passed")


def main() -> int:
    work = os.path.join(tempfile.gettempdir(), "pointwise_crash_verify")
    os.makedirs(work, exist_ok=True)
    for f in os.listdir(work):
        os.remove(os.path.join(work, f))
    pid = os.fork()
    if pid == 0:
        stage_crash(work)
    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        code = os.WEXITSTATUS(status)
    else:
        code = -1
    assert code == 99, f"崩溃子进程退出码异常: {code}"
    assert os.path.exists(os.path.join(work, "cap.db"))
    stage_resume(work)
    return 0


if __name__ == "__main__":
    sys.exit(main())
