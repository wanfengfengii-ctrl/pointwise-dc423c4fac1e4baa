"""capture.py 契约回归测试。"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from capture import C, CaptureError, S

SEED = {"d": {"x": 0, "y": 0, "note": None}}


class StoreBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cap_test_")
        self.sdb = os.path.join(self.tmp, "s.db")
        self.cdb = os.path.join(self.tmp, "c.db")

    def paths(self, name):
        return os.path.join(self.tmp, name + ".db")


class TestSourceStore(StoreBase):
    def test_build_reopen_none_and_close(self):
        s = S(self.sdb, [["b", "t1"]], sid="A")
        self.assertEqual(s.sid, "A")
        self.assertEqual(s.acked, 0)
        s.close()
        s2 = S(self.sdb, None)
        self.assertEqual(s2.sid, "A")
        self.assertEqual(s2.next(0), (1, ["b", "t1"]))
        s2.close()

    def test_double_seed_and_reopen_without_seed_rejected(self):
        S(self.sdb, [], sid="A").close()
        with self.assertRaises(CaptureError):
            S(self.sdb, [], sid="A")
        with self.assertRaises(CaptureError):
            S(self.paths("fresh"), None)

    def test_next_after_i_and_end_none(self):
        s = S(self.sdb, [["a"], ["b", "t1"], ["a"]], sid="A")
        self.assertEqual(s.next(0), (1, ["a"]))
        self.assertEqual(s.next(1), (2, ["b", "t1"]))      # i 之后一条
        self.assertEqual(s.next(3), None)                  # 尽头 None
        self.assertEqual(s.next(None), (1, ["a"]))

    def test_next_rejects_below_acked(self):
        s = S(self.sdb, [["a"], ["b", "t1"]], sid="A")
        self.assertEqual(s.ack(1), 1)
        self.assertEqual(s.acked, 1)
        with self.assertRaises(CaptureError):
            s.next(0)                                        # i < acked 拒读
        self.assertEqual(s.next(1), (2, ["b", "t1"]))

    def test_ack_atomic_idempotent_delete(self):
        s = S(self.sdb, [["a"], ["a"], ["b", "t1"]], sid="A")
        self.assertEqual(s.ack(2), 2)
        self.assertEqual(s.ack(2), 0)                        # 幂等无新增
        self.assertEqual(s.ack(1), 0)                        # 回退位置 no-op
        rows = s._conn.execute("SELECT pos FROM events ORDER BY pos").fetchall()
        self.assertEqual(rows, [(3,)])
        raw = sqlite3.connect(self.sdb)
        self.assertEqual(raw.execute("SELECT v FROM meta WHERE k='acked'").fetchone()[0], "2")
        raw.close()

    def test_sid_mismatch_on_reopen(self):
        S(self.sdb, [], sid="A").close()
        with self.assertRaises(CaptureError):
            S(self.sdb, None, sid="B")


class TestValidation(StoreBase):
    def _pump(self, events, sid="X"):
        db = self.paths("src_" + sid + "_" + str(next(self._seq)))
        s = S(db, events, sid=sid)
        c = C(None, SEED)
        return c, s

    def setUp(self):
        super().setUp()
        self._seq = iter(range(10000))

    def test_illegal_shapes_do_not_advance(self):
        for bad in ([], "x", ["z"], ["b", 1], ["b", ""], ["b", "t", 9],
                    ["s", "t"], ["s", "t", "u", "/nope"],
                    ["w", "t", "/nope", "k", {"x": 1}],
                    ["w", "t", "", "k", {}],
                    ["w", "t", "", "k", {"z": 1}],
                    ["w", "t", "", "k", {"x": "1"}],
                    ["w", "t", "", "k", {"note": 5}],
                    ["r", "t", ""], ["r", "t", "/gone"],
                    ["c"], ["a", 1], 42):
            c, s = self._pump([bad, ["b", "ok" + str(id(bad))]])
            with self.assertRaises(CaptureError):
                c.pump(s, n=None)
            self.assertEqual(c.read()[0], 0)
            self.assertEqual(s.acked, 0)
            c.close()
            s.close()

    def test_tag_and_domain_numbers_never_reused(self):
        events = [
            ["b", "t1"],
            ["s", "t2", "u1", "/t1"],
            ["r", "t3", "/t1/u1"],
            ["r", "t4", "/t1"],
            ["b", "t1"],                                     # 父号复用
        ]
        c, s = self._pump(events)
        with self.assertRaises(CaptureError):
            c.pump(s, n=None)
        self.assertEqual(c.read()[0], 4)                     # 前 4 条已落盘
        c.close()
        s.close()

        events2 = [
            ["b", "p1"],
            ["s", "p2", "u", "/p1"],
            ["r", "p3", "/p1/u"],
            ["s", "p4", "u", "/p1"],                         # 父内子号复用
        ]
        c, s = self._pump(events2, sid="Y")
        with self.assertRaises(CaptureError):
            c.pump(s, n=None)
        self.assertEqual(c.read()[0], 3)

    def test_preview_rejects_used_or_bad_tag_but_does_not_consume(self):
        c, s = self._pump([["b", "t1"], ["c", "c1"]])
        c.pump(s, n=None)
        with self.assertRaises(CaptureError):
            c.preview("c1")
        with self.assertRaises(CaptureError):
            c.preview("")
        self.assertEqual(c.preview("free"), [["d", [0, 0, None]]])


class TestOverlayAndRetract(StoreBase):
    def test_seed_order_null_write_and_survivors(self):
        seed = {
            "a": {"x": 1, "y": 2, "note": "s"},
            "d": {"x": 0, "y": 0, "note": None},
        }
        events = [
            ["b", "t1"],
            ["s", "t2", "u1", "/t1"],
            ["w", "t3", "", "d", {"x": 2}],
            ["w", "t4", "/t1/u1", "d", {"y": 90, "note": None}],
            ["w", "t5", "", "a", {"x": 5, "note": None}],    # null 可写
            ["r", "t6", "/t1"],                              # 孙域 y=90 失效
        ]
        s = S(self.sdb, events, sid="A")
        c = C(self.cdb, seed)
        c.pump(s, n=None)
        _, _, rows, receipts = c.read()
        self.assertEqual(rows, [
            ["a", [5, 2, None]],
            ["d", [2, 0, None]],
        ])
        self.assertEqual(receipts, [])
        self.assertEqual(c.preview("pv"), rows)

    def test_retract_kills_subtree_patches_and_domain_refs(self):
        events = [
            ["b", "t1"],
            ["w", "t2", "/t1", "d", {"x": 9}],
            ["r", "t3", "/t1"],
            ["w", "t4", "/t1", "d", {"x": 8}],               # 引用已撤域
        ]
        s = S(self.sdb, events, sid="A")
        c = C(self.cdb, SEED)
        with self.assertRaises(CaptureError):
            c.pump(s, n=None)
        self.assertEqual(c.read()[2], [["d", [0, 0, None]]])

    def test_new_key_defaults_and_event_order_overlay(self):
        events = [
            ["w", "t1", "", "k2", {"y": 4}],
            ["w", "t2", "", "k2", {"y": 5}],
            ["w", "t3", "", "k2", {"note": "n"}],
        ]
        s = S(self.sdb, events, sid="A")
        c = C(self.cdb, SEED)
        c.pump(s, n=None)
        self.assertEqual(c.read()[2], [
            ["d", [0, 0, None]],
            ["k2", [0, 5, "n"]],
        ])


class TestCommitReceipts(StoreBase):
    def test_empty_commit_emits_receipt_and_snapshot_frozen(self):
        events = [
            ["c", "c0"],                                     # 空提交也发
            ["w", "t1", "", "d", {"x": 2}],
            ["c", "c1"],
            ["w", "t2", "", "d", {"x": 7}],
        ]
        s = S(self.sdb, events, sid="A")
        c = C(self.cdb, SEED)
        c.pump(s, n=None)
        _, _, rows, receipts = c.read()
        self.assertEqual(rows, [["d", [7, 0, None]]])
        self.assertEqual(len(receipts), 2)
        self.assertEqual(receipts[0], ["c0", 1, [["d", [0, 0, None]]]])
        self.assertEqual(receipts[1], ["c1", 3, [["d", [2, 0, None]]]])
        # 回执按提交序冻结：后续写入不改变旧回执
        self.assertEqual(receipts[1][2], [["d", [2, 0, None]]])

    def test_hooks_around_commit_and_after_ack_exceptions_propagate(self):
        events = [
            ["w", "t1", "", "d", {"x": 2}],
            ["c", "c1"],
            ["a"],
        ]
        s = S(self.sdb, events, sid="A")
        c = C(self.cdb, SEED)
        log = []
        c.pump(s, n=None, h=lambda p, i, e: log.append((p, i, e[0] if e else None)))
        self.assertEqual(log, [(0, 2, "c"), (1, 2, "c"), (2, 3, "a")])
        c.close()
        s.close()

        class Boom(Exception):
            pass

        for phase in (0, 1, 2):
            s = S(self.paths(f"h{phase}"), [["c", "x"], ["a"]], sid="H")
            c = C(None, SEED)

            def hook(p, i, e, phase=phase):
                if p == phase:
                    raise Boom(phase)

            with self.assertRaises(Boom):
                c.pump(s, n=None, h=hook)
            c.close()
            s.close()


class TestAckAndRestart(StoreBase):
    def test_ack_without_commit_and_durable_restart(self):
        events = [["w", "t1", "", "d", {"x": 2}], ["a"], ["b", "t2"]]
        s = S(self.sdb, events, sid="A")
        c = C(self.cdb, SEED)
        self.assertEqual(c.pump(s, n=2), 2)
        self.assertEqual(s.acked, 2)
        self.assertEqual(c.read()[:2], [2, 2])
        c.close()
        s.close()

        # 模拟中断后重开：n=0 先补确认（幂等），无事件重复消费
        s2 = S(self.sdb, None)
        c2 = C(self.cdb, None)
        hooks = []
        self.assertEqual(c2.pump(s2, n=0, h=lambda p, i, e: hooks.append((p, i))), 0)
        self.assertEqual(hooks, [(2, 2)])
        self.assertEqual(s2.acked, 2)
        self.assertEqual(c2.pump(s2, n=None), 1)
        self.assertEqual(c2.read()[2], [["d", [2, 0, None]]])
        c2.close()
        s2.close()

    def test_source_events_deleted_only_after_local_durable(self):
        events = [["w", "t1", "", "d", {"x": 1}], ["a"]]
        s = S(self.sdb, events, sid="A")
        c = C(self.cdb, SEED)
        c.pump(s, n=1)                                        # 未遇 [a]，不确认
        self.assertEqual(s.acked, 0)
        self.assertEqual(s.next(1), (2, ["a"]))
        c.pump(s, n=1)
        self.assertEqual(s.acked, 2)
        self.assertIsNone(s.next(2))

    def test_limit_none_and_n_zero(self):
        s = S(self.sdb, [["b", f"t{i}"] for i in range(5)], sid="A")
        c = C(self.cdb, SEED)
        self.assertEqual(c.pump(s, n=0), 0)
        self.assertEqual(c.pump(s, n=2), 2)
        self.assertEqual(c.read()[0], 2)
        self.assertEqual(c.pump(s, n=None), 3)
        self.assertEqual(c.read()[0], 5)
        self.assertEqual(c.pump(s, n=None), 0)               # 尽头 0 新收


class TestSourceSwitch(StoreBase):
    def test_switch_rejected_with_pending_allowed_after_full_ack(self):
        s1 = S(self.sdb, [["b", "t1"], ["a"]], sid="A")
        c = C(self.cdb, SEED)
        c.pump(s1, n=1)                                      # 未确认
        other = S(self.paths("other"), [["b", "q"]], sid="B")
        with self.assertRaises(CaptureError):
            c.pump(other, n=None)
        self.assertEqual(c.read()[0], 1)
        c.pump(s1, n=1)                                      # 确认完成
        self.assertEqual(s1.acked, 2)
        # 旧源排空后允许换 B；位置从 1 重新编号，历史仓行保留
        c.pump(other, n=None)
        self.assertEqual(c.read()[:2], [1, 0])
        c.close()

    def test_switch_keeps_warehouse_and_rebased_positions(self):
        s1 = S(self.sdb, [["w", "t1", "", "d", {"x": 2}], ["a"]], sid="A")
        c = C(self.cdb, SEED)
        c.pump(s1, n=None)
        self.assertEqual(s1.acked, 2)
        s2 = S(self.paths("b"), [
            ["b", "b1"],
            ["w", "b2", "/b1", "d", {"y": 3, "note": "B"}],
            ["c", "b3"],
            ["a"],
        ], sid="B")
        c.pump(s2, n=None)
        _, _, rows, receipts = c.read()
        self.assertEqual(rows, [["d", [2, 3, "B"]]])
        self.assertEqual(receipts[0][:2], ["b3", 3])
        # 全局位置不冲突：applied 表 gpos 接续
        gpos = [r[0] for r in c._conn.execute("SELECT gpos FROM applied ORDER BY gpos")]
        self.assertEqual(gpos, [1, 2, 3, 4, 5, 6])
        self.assertEqual(s2.acked, 4)


class TestMemoryStoreAndSeedChecks(StoreBase):
    def test_memory_reopen_rejected(self):
        with self.assertRaises(CaptureError):
            S(None, None)
        with self.assertRaises(CaptureError):
            C(None, None)

    def test_bad_seed(self):
        for bad in ([], {"k": {"x": "1", "y": 0, "note": None}},
                    {"k": {"x": 1, "y": 0, "note": 1}},
                    {"k": {"x": 1, "y": 0}},
                    {"k": {"x": True, "y": 0, "note": None}}):
            with self.assertRaises(CaptureError):
                C(None, bad)


if __name__ == "__main__":
    unittest.main(verbosity=2)
