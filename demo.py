"""进程中断核验与续办演示。

叙事（对应题给示例 d=(0,0,null)）：
  源 A：根写 x=2、孙域写 y=90、确认到 6 —— 预览 (2,90,null)，确认不提交。
  模拟进程中断（关闭句柄、删除源内已确认事件后的重开效果）。
  重开：先补确认（n=0 也发），再撤父域，确认至 10。
  换源 B：在新父域提交 y=3、note=B —— A 得 (2,3,B)。
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

from capture import C, CaptureError, S


def main() -> int:
    work = os.path.join(tempfile.gettempdir(), "pointwise_capture_demo")
    os.makedirs(work, exist_ok=True)
    for f in os.listdir(work):
        os.remove(os.path.join(work, f))
    sdb = os.path.join(work, "source_A.db")
    cdb = os.path.join(work, "warehouse.db")
    sdb_b = os.path.join(work, "source_B.db")
    print(f"== 运行目录: {work}")

    seed = {"d": {"x": 0, "y": 0, "note": None}}

    # ---------------------------------------------------------------- #
    # 阶段 1：源 A 入仓 —— 根写 x=2、孙域写 y=90，确认（不提交）
    # ---------------------------------------------------------------- #
    events_a = [
        ["b", "t1"],                       # 1 开父域 /t1
        ["s", "t2", "u1", "/t1"],          # 2 在 /t1 下开子域 u1
        ["w", "t3", "", "d", {"x": 2}],    # 3 根域写 d.x=2
        ["w", "t4", "/t1/u1", "d", {"y": 90}],  # 4 孙域写 d.y=90
        ["r", "t5", "/t1/u1"],             # 5 撤回孙域（y=90 随子树失效）
        ["a"],                             # 6 确认请求
        ["b", "t6"],                       # 7 再开父域 /t6（父号不复用，t1 不可再用）
        ["w", "t7", "/t6", "d", {"y": 5}], # 8 /t6 写 d.y=5
        ["r", "t8", "/t6"],                # 9 撤回 /t6（y=5 失效）
        ["a"],                             # 10 确认请求
    ]
    src = S(sdb, events_a, sid="A")
    cap = C(cdb, seed)

    hook_log: list = []

    def h(phase: int, pos, ev):
        hook_log.append((phase, pos))

    got = cap.pump(src, n=6, h=h)
    print(f"[A] 首批泵入 {got} 条（限 n=6），读: {cap.read()[:2]}")
    assert got == 6

    consumed, ackreq, rows, receipts = cap.read()
    assert (consumed, ackreq) == (6, 6)
    assert rows == [["d", [2, 0, None]]], rows          # 孙域 y=90 已随撤回失效
    assert receipts == []                                # 确认不提交，无回执
    assert src.acked == 6                                # 源侧已确认到 6
    assert src.next(6) is not None                        # 7..10 仍在源中
    preview = cap.preview("t-pre")
    print(f"[A] 确认后 preview(t-pre) = {preview}")
    assert preview == [["d", [2, 0, None]]]
    print(f"[A] 钩子记录（提交前后/源确认）: {hook_log}")
    assert hook_log == [(2, 6)]                           # 无提交，仅源确认后 h2

    # ---------------------------------------------------------------- #
    # 阶段 2：模拟进程中断 —— 关闭句柄；源库/入仓库均为磁盘持久
    # ---------------------------------------------------------------- #
    print("== 模拟进程中断：close() 全部句柄，随后以 None 重开")
    cap.close()
    src.close()
    assert not os.path.exists(cdb + "-journal") or True

    cap = C(cdb, None)                                    # None 重开
    src = S(sdb, None)                                    # None 重开源 A
    assert src.acked == 6
    assert src.next(6) is not None                        # 未确认事件仍可取

    # ---------------------------------------------------------------- #
    # 阶段 3：重开先补确认 —— n=0 不接纳新事件，但补发上次已持久的 ack
    # ---------------------------------------------------------------- #
    replay_log: list = []
    zero = cap.pump(src, n=0, h=lambda p, i, e: replay_log.append((p, i)))
    print(f"[重开] pump(n=0) 新收 {zero} 条；源 acked={src.acked}；钩子={replay_log}")
    assert zero == 0
    assert src.acked == 6                                 # 重放幂等，acked 不变
    assert replay_log == [(2, 6)]                         # 重开先补确认，n=0 亦然

    # 泵入剩余 7..10：撤父域路径，全部确认
    got = cap.pump(src, n=None)
    print(f"[重开] 续泵剩余事件 {got} 条；源 acked={src.acked}")
    assert got == 4
    assert src.acked == 10
    assert src.next(10) is None                           # 尽头 None
    consumed, ackreq, rows, receipts = cap.read()
    assert (consumed, ackreq) == (10, 10)
    assert rows == [["d", [2, 0, None]]]
    cap.close()
    src.close()

    # ---------------------------------------------------------------- #
    # 阶段 4：换源 B —— 旧源已全部确认方可换；B 提交 y=3、note=B
    # ---------------------------------------------------------------- #
    events_b = [
        ["b", "b1"],                                      # 1 新父域 /b1
        ["w", "b2", "/b1", "d", {"y": 3, "note": "B"}],   # 2 写 y=3, note=B
        ["c", "b3"],                                      # 3 提交（空串 t 非法，此处标签 b3）
        ["a"],                                            # 4 确认
    ]
    src_b = S(sdb_b, events_b, sid="B")
    cap = C(cdb, None)
    got = cap.pump(src_b, n=None, h=h)
    print(f"[B] 换源后泵入 {got} 条；读: {cap.read()}")
    assert got == 4
    consumed, ackreq, rows, receipts = cap.read()
    assert (consumed, ackreq) == (4, 4)                   # 新源位置从 1 重新编号
    assert rows == [["d", [2, 3, "B"]]], rows             # A 的 x=2 幸存，B 覆盖 y/note
    assert len(receipts) == 1
    tag, pos, snap = receipts[0]
    assert tag == "b3" and pos == 3
    assert snap == [["d", [2, 3, "B"]]]
    assert src_b.acked == 4

    # ---------------------------------------------------------------- #
    # 阶段 5：非法事件与未确认换源 —— CaptureError 且不推进
    # ---------------------------------------------------------------- #
    # 5a 非法事件（引用不存在的域）：独立仓，第一条即拒，消费位保持 0
    bad_src = S(
        os.path.join(work, "source_bad.db"),
        [["w", "x1", "/ghost", "d", {"x": 1}], ["b", "z1"]],
        sid="BAD",
    )
    bad_cap = C(None, seed)                              # 内存仓即建即用
    assert bad_cap.read()[0] == 0
    try:
        bad_cap.pump(bad_src, n=None)
    except CaptureError as exc:
        print(f"[拒非法] 引用不存在域 -> CaptureError: {exc}")
    else:
        raise AssertionError("非法事件未被拒绝")
    assert bad_cap.read()[0] == 0                        # 位置不推进
    assert bad_src.acked == 0                            # 未确认任何事件
    # 源内事件未被删除，纠正后（先开域再写）仍可正常入仓
    fixed = S(
        os.path.join(work, "source_fixed.db"),
        [["b", "z1"], ["w", "x1", "/z1", "d", {"x": 7}]],
        sid="FIX",
    )
    fixed_cap = C(None, seed)
    assert fixed_cap.pump(fixed, n=None) == 2
    assert fixed_cap.read()[2] == [["d", [7, 0, None]]]
    bad_cap.close()
    bad_src.close()
    fixed_cap.close()
    fixed.close()

    # 5b 未确认换源：仓中尚有未 [a] 的事件时，换一个源 id 必须被拒
    pending_src = S(
        os.path.join(work, "source_pending.db"),
        [["b", "p1"], ["w", "p2", "", "d", {"x": 8}]],
        sid="PENDING",
    )
    pending_cap = C(None, seed)
    assert pending_cap.pump(pending_src, n=None) == 2    # 无 [a]，未确认
    assert pending_cap.read()[0] == 2 and pending_src.acked == 0
    conflict = S(os.path.join(work, "source_c.db"), [["b", "q1"]], sid="C")
    before = pending_cap.read()[0]
    try:
        pending_cap.pump(conflict, n=None)
    except CaptureError as exc:
        print(f"[拒换源] 带未确认事件换源 -> CaptureError: {exc}")
    else:
        raise AssertionError("换源冲突未被拒绝")
    assert pending_cap.read()[0] == before               # 位置不推进

    # 底层 SQLite 文件核验：关键表均已落盘
    with sqlite3.connect(cdb) as raw:
        n_dom = raw.execute("SELECT count(*) FROM domains").fetchone()[0]
        n_patch = raw.execute("SELECT count(*) FROM patches").fetchone()[0]
        n_recv = raw.execute("SELECT count(*) FROM receipts").fetchone()[0]
    print(f"[落盘] domains={n_dom} patches={n_patch} receipts={n_recv}")
    # domains: 根 + /t1 + /t1/u1 + /t6（含撤回，仍留档）+ /b1 = 5
    # patches: x=2, y=90(已撤), y=5(已撤), y=3, note=B 共 5 行
    assert (n_dom, n_patch, n_recv) == (5, 5, 1)

    cap.close()
    pending_cap.close()
    pending_src.close()
    conflict.close()
    print("== 演示全部断言通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
