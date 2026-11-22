"""入仓（capture）模块：源事件库 S 与入仓库 C，SQLite 持久化，可崩溃续办。

事件约定（ev 从 1 编号，列表形式）：
  [b, t]            在根下开父域，域 id 为 "/<t>"
  [s, t, u, p]      在活父域 p 下开子域 u，域 id 为 p + "/" + u
  [w, t, u, k, 补丁] 在活域 u 内对键 k 写补丁 {x?, y?, note?}，note 可写 null
  [r, t, u]         撤回以活域 u 为根的子树（根域 "" 不可撤）
  [c, t]            提交：冻结回执 [t, 提交事件位置, 按键排的 [键, 整行]]，空提交也发
  [a]               确认请求：本地持久后，向源确认到当前位置

通用规则：
  - 空串 "" 为根域；所有被引用的域必须是活域。
  - 域标识路径中的父号、父内子号不复用（撤回后也不可再用）。
  - 补丁按事件序叠加；被撤回子树内的补丁不再幸存；null 可写入。
  - 非法事件或带未确认事件换源 -> CaptureError，且不推进任何已持久位置。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable, Optional

__all__ = ["CaptureError", "S", "C"]

_ROOT = ""
_SEP = "/"
_COLUMNS = ("x", "y", "note")
_HOOK_TAGS = frozenset("bswrc")


class CaptureError(Exception):
    """非法事件、非法引用、换源冲突或库初始化冲突。"""


# --------------------------------------------------------------------------- #
# 源事件库（分库 1）：只存原始事件、绑定的源 id、已确认位 acked
# --------------------------------------------------------------------------- #
class S:
    """Source store。S(db, seed) 建库；S(db, None) 重开；close() 关闭。"""

    def __init__(self, db: Optional[str], seed: Optional[list], sid: str = "source"):
        if db is None:
            db = ":memory:"
        fresh = seed is not None
        if not fresh and db == ":memory:":
            raise CaptureError("内存库无法以 None 重开，必须提供 seed")
        self._closed = False
        self._conn = sqlite3.connect(db)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            " pos INTEGER PRIMARY KEY CHECK (pos >= 1), ev TEXT NOT NULL)"
        )
        self._conn.commit()
        row = self._conn.execute("SELECT v FROM meta WHERE k='sid'").fetchone()
        if fresh:
            if row is not None:
                raise CaptureError("源库已初始化，重开请传 None")
            if not isinstance(seed, list):
                raise CaptureError("seed 必须是事件列表")
            if not isinstance(sid, str) or not sid:
                raise CaptureError("源 id 必须是非空字符串")
            with self._conn:
                self._conn.execute(
                    "INSERT INTO meta(k,v) VALUES ('sid', ?)", (sid,)
                )
                self._conn.execute(
                    "INSERT INTO meta(k,v) VALUES ('acked', '0')"
                )
                self._conn.executemany(
                    "INSERT INTO events(pos, ev) VALUES (?, ?)",
                    [(i, json.dumps(ev, ensure_ascii=False)) for i, ev in enumerate(seed, 1)],
                )
        else:
            if row is None:
                raise CaptureError("源库尚未初始化，无法以 None 重开")
            if sid != "source" and sid != row[0]:
                raise CaptureError(
                    f"源 id 不匹配：库内为 {row[0]!r}，传入为 {sid!r}"
                )
        self._sid = self._conn.execute(
            "SELECT v FROM meta WHERE k='sid'"
        ).fetchone()[0]
        self._acked = int(
            self._conn.execute("SELECT v FROM meta WHERE k='acked'").fetchone()[0]
        )

    @property
    def sid(self) -> str:
        return self._sid

    @property
    def acked(self) -> int:
        return self._acked

    def next(self, i: Optional[int]):
        """返回位置 i 之后的第一条 (pos, ev)，尽头返回 None。i < acked 拒读。"""
        self._check_open()
        if i is None:
            i = 0
        if i < 0:
            raise CaptureError("位置不能为负")
        if i < self._acked:
            raise CaptureError(
                f"位置 {i} 的事件已确认删除（acked={self._acked}），拒读"
            )
        row = self._conn.execute(
            "SELECT pos, ev FROM events WHERE pos > ? ORDER BY pos LIMIT 1", (i,)
        ).fetchone()
        if row is None:
            return None
        return row[0], json.loads(row[1])

    def ack(self, i: int) -> int:
        """原子地删除 1..i 事件并推进 acked；幂等，返回新删除条数。"""
        self._check_open()
        if i is None or i < 0:
            raise CaptureError("确认位置非法")
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur_acked = int(
                self._conn.execute(
                    "SELECT v FROM meta WHERE k='acked'"
                ).fetchone()[0]
            )
            if i <= cur_acked:
                self._conn.execute("COMMIT")
                return 0
            deleted = self._conn.execute(
                "DELETE FROM events WHERE pos <= ?", (i,)
            ).rowcount
            self._conn.execute(
                "UPDATE meta SET v=? WHERE k='acked'", (str(i),)
            )
            self._conn.execute("COMMIT")
            self._acked = i
            return deleted
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._closed = True

    def _check_open(self) -> None:
        if self._closed:
            raise CaptureError("源库已关闭")


# --------------------------------------------------------------------------- #
# 入仓库（分库 2）：域树、补丁、回执、消费/确认位、源绑定
# --------------------------------------------------------------------------- #
class C:
    """Capture/warehouse store。C(db, seed) 建库；C(db, None) 重开。"""

    def __init__(self, db: Optional[str], seed: Optional[dict]):
        if db is None:
            db = ":memory:"
        fresh = seed is not None
        if not fresh and db == ":memory:":
            raise CaptureError("内存库无法以 None 重开，必须提供 seed")
        self._closed = False
        self._conn = sqlite3.connect(db)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()
        self._conn.commit()
        initialized = self._conn.execute(
            "SELECT 1 FROM meta WHERE k='seed'"
        ).fetchone()
        if fresh:
            if initialized:
                raise CaptureError("入仓库已初始化，重开请传 None")
            self._validate_seed(seed)
            with self._conn:
                self._conn.execute(
                    "INSERT INTO meta(k,v) VALUES ('seed', ?)",
                    (json.dumps(seed, ensure_ascii=False),),
                )
                for k, v in (
                    ("consumed", "0"),
                    ("ackreq", "0"),
                    ("last_ack", "0"),
                    ("pos_base", "0"),
                    ("sid", ""),
                ):
                    self._conn.execute(
                        "INSERT INTO meta(k,v) VALUES (?, ?)", (k, v)
                    )
                # 根域常驻、存活、不可撤回
                self._conn.execute(
                    "INSERT INTO domains(path, parent, name, alive, born_gpos) "
                    "VALUES ('', NULL, '', 1, 0)"
                )
        else:
            if not initialized:
                raise CaptureError("入仓库尚未初始化，无法以 None 重开")
        m = dict(
            (k, v)
            for k, v in self._conn.execute("SELECT k, v FROM meta")
        )
        self._seed = json.loads(m["seed"])
        self._consumed = int(m["consumed"])
        self._ackreq = int(m["ackreq"])
        self._last_ack = int(m["last_ack"])
        self._pos_base = int(m["pos_base"])
        self._sid = m["sid"] or None

    # ------------------------------------------------------------------ #
    # 初始化与基础结构
    # ------------------------------------------------------------------ #
    def _create_schema(self) -> None:
        c = self._conn
        c.execute(
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS domains ("
            " path TEXT PRIMARY KEY,"
            " parent TEXT REFERENCES domains(path),"
            " name TEXT NOT NULL,"
            " alive INTEGER NOT NULL,"
            " born_gpos INTEGER NOT NULL)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS tags ("
            " tag TEXT PRIMARY KEY, kind TEXT NOT NULL, gpos INTEGER NOT NULL)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS applied ("
            " gpos INTEGER PRIMARY KEY, local_pos INTEGER NOT NULL,"
            " sid TEXT NOT NULL, ev TEXT NOT NULL)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS patches ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " domain TEXT NOT NULL,"
            " key TEXT NOT NULL,"
            " col TEXT NOT NULL,"
            " val TEXT,"
            " born_gpos INTEGER NOT NULL,"
            " born_local INTEGER NOT NULL,"
            " retract_gpos INTEGER)"
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_patches ON patches(key, born_local)")
        c.execute(
            "CREATE TABLE IF NOT EXISTS receipts ("
            " gpos INTEGER PRIMARY KEY,"
            " sid TEXT NOT NULL,"
            " local_pos INTEGER NOT NULL,"
            " tag TEXT NOT NULL,"
            " content TEXT NOT NULL)"
        )

    @staticmethod
    def _validate_seed(seed: Any) -> None:
        if not isinstance(seed, dict):
            raise CaptureError("seed 必须是 {键: {x:int,y:int,note:str|null}} 字典")
        for key, row in seed.items():
            if not isinstance(key, str) or not isinstance(row, dict):
                raise CaptureError("seed 的键必须是字符串，值必须是字典")
            if not isinstance(row.get("x"), int) or isinstance(row.get("x"), bool):
                raise CaptureError(f"seed[{key!r}].x 必须是 int")
            if not isinstance(row.get("y"), int) or isinstance(row.get("y"), bool):
                raise CaptureError(f"seed[{key!r}].y 必须是 int")
            note = row.get("note")
            if note is not None and not isinstance(note, str):
                raise CaptureError(f"seed[{key!r}].note 必须是 str 或 null")
            if set(row) != set(_COLUMNS):
                raise CaptureError(f"seed[{key!r}] 列必须恰为 x,y,note")

    # ------------------------------------------------------------------ #
    # 只读接口
    # ------------------------------------------------------------------ #
    def read(self) -> list:
        """返回 [消费位, 确认请求位, 仓行(按键排的[键,整行]), 回执表]。"""
        self._check_open()
        rows = self._snapshot(self._consumed)
        receipts = [
            [tag, local_pos, json.loads(content)]
            for tag, local_pos, content in self._conn.execute(
                "SELECT tag, local_pos, content FROM receipts ORDER BY gpos"
            )
        ]
        return [self._consumed, self._ackreq, rows, receipts]

    def preview(self, t: str) -> list:
        """以合法且未使用的 t 假设此刻提交，返回按键排的 [键, 整行] 快照。

        只校验不消费 t；按事件序向现仓叠加全部幸存补丁，与真实提交同一投影逻辑。
        """
        self._check_open()
        if not isinstance(t, str) or not t:
            raise CaptureError("预览标签 t 必须是非空字符串")
        if self._tag_exists(t):
            raise CaptureError(f"标签 {t!r} 已被使用，不可复用")
        return self._snapshot(self._consumed)

    # ------------------------------------------------------------------ #
    # 泵入
    # ------------------------------------------------------------------ #
    def pump(self, s: "S", n: Optional[int], h: Optional[Callable] = None) -> int:
        """从源 s 拉取至多 n 条事件并入仓；n=None 不限；返回新收条数。

        - 绑源 id：首次接纳绑定；带未确认事件换源 -> CaptureError 且不推进。
        - 重开先补确认（last_ack 重放），n=0 亦然。
        - h(0,i,ev) 接纳提交前；h(1,i,ev) 提交持久后；h(2,i,ev) 源确认后。
          钩子异常原样透传。
        """
        self._check_open()
        if n is not None and (not isinstance(n, int) or isinstance(n, bool) or n < 0):
            raise CaptureError("n 必须是非负整数或 None")

        # 1) 源 id 绑定 / 换源校验（先绑源，避免把旧源的 ack 误发到新源）。
        if self._sid is None:
            self._set_meta("sid", s.sid)
            self._sid = s.sid
        elif self._sid != s.sid:
            # 仅当旧源事件全部确认、全部消费后，才允许换源（位置归零重新编号）。
            if self._consumed != self._last_ack or self._consumed == 0:
                raise CaptureError(
                    f"换源冲突：已绑 {self._sid!r}，仍有未确认事件，"
                    f"拒绝 {s.sid!r}，位置不推进"
                )
            self._pos_base += self._consumed
            self._consumed = self._ackreq = self._last_ack = 0
            self._set_meta("pos_base", str(self._pos_base))
            self._set_meta("consumed", "0")
            self._set_meta("ackreq", "0")
            self._set_meta("last_ack", "0")
            self._set_meta("sid", s.sid)
            self._sid = s.sid

        # 2) 重开先补确认：已本地持久但尚未送达“同一源”的 ack 补发（幂等）。
        #    n=0 也会执行；换源后 last_ack 已归零，不会误 ack 新源。
        if self._last_ack > 0 and s.sid == self._sid:
            s.ack(self._last_ack)
            if h is not None:
                h(2, self._last_ack, None)

        # 3) 按限额拉取并逐条事务接纳。
        ingested = 0
        while n is None or ingested < n:
            item = s.next(self._consumed)
            if item is None:
                break
            local_pos, ev = item
            gpos = self._pos_base + local_pos
            self._validate(ev)
            kind = ev[0]
            if kind == "c":
                if h is not None:
                    h(0, local_pos, ev)
            self._apply(gpos, local_pos, ev)
            if kind == "c":
                if h is not None:
                    h(1, local_pos, ev)
            ingested += 1

            # 4) 确认请求：本地事务已提交才 ack 源；ack 位持久，崩溃可补发。
            if kind == "a":
                s.ack(local_pos)
                self._last_ack = local_pos
                self._set_meta("last_ack", str(local_pos))
                if h is not None:
                    h(2, local_pos, ev)
        return ingested

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._closed = True

    # ------------------------------------------------------------------ #
    # 事件校验（结构 + 活域引用 + 号不复用 + 标签不复用）
    # ------------------------------------------------------------------ #
    def _validate(self, ev: Any) -> None:
        if not isinstance(ev, (list, tuple)) or not ev:
            raise CaptureError(f"非法事件（必须是非空列表）: {ev!r}")
        kind = ev[0]
        if kind not in _HOOK_TAGS and kind != "a":
            raise CaptureError(f"非法事件类型 {kind!r}: {ev!r}")

        if kind == "a":
            if len(ev) != 1:
                raise CaptureError(f"[a] 确认事件不能携带字段: {ev!r}")
            return

        if len(ev) < 2 or not isinstance(ev[1], str) or not ev[1]:
            raise CaptureError(f"事件标签 t 必须是非空字符串: {ev!r}")
        tag = ev[1]
        if self._tag_exists(tag):
            raise CaptureError(f"标签 {tag!r} 已被使用，不可复用: {ev!r}")

        if kind == "b":
            if len(ev) != 2:
                raise CaptureError(f"[b,t] 字段数必须为 2: {ev!r}")
            name = tag
            if _SEP in name:
                raise CaptureError(f"域名不能包含 {_SEP!r}: {ev!r}")
            if self._domain_known(_SEP + name):
                raise CaptureError(f"父域号 {name!r} 已存在（号不复用）: {ev!r}")
        elif kind == "s":
            if len(ev) != 4:
                raise CaptureError(f"[s,t,u,p] 字段数必须为 4: {ev!r}")
            _, _, u, p = ev
            if not isinstance(u, str) or not u or _SEP in u:
                raise CaptureError(f"子域名 u 必须是非空且不含 {_SEP!r}: {ev!r}")
            if not isinstance(p, str):
                raise CaptureError(f"父域 p 必须是字符串: {ev!r}")
            if not self._domain_live(p):
                raise CaptureError(f"父域 {p!r} 不存在或已撤回: {ev!r}")
            if self._domain_known(p + _SEP + u):
                raise CaptureError(
                    f"父域 {p!r} 下子号 {u!r} 已存在（号不复用）: {ev!r}"
                )
        elif kind == "w":
            if len(ev) != 5:
                raise CaptureError(f"[w,t,u,k,patch] 字段数必须为 5: {ev!r}")
            _, _, u, key, patch = ev
            if not isinstance(u, str) or not self._domain_live(u):
                raise CaptureError(f"写事件引用的域 {u!r} 不存在或已撤回: {ev!r}")
            if not isinstance(key, str):
                raise CaptureError(f"键 k 必须是字符串: {ev!r}")
            if not isinstance(patch, dict) or not patch:
                raise CaptureError(f"补丁必须是非空字典: {ev!r}")
            extra = set(patch) - set(_COLUMNS)
            if extra:
                raise CaptureError(f"补丁含非法列 {sorted(extra)}: {ev!r}")
            for col in ("x", "y"):
                if col in patch:
                    v = patch[col]
                    if not isinstance(v, int) or isinstance(v, bool):
                        raise CaptureError(f"补丁列 {col} 必须是 int: {ev!r}")
            if "note" in patch and patch["note"] is not None:
                if not isinstance(patch["note"], str):
                    raise CaptureError(f"补丁列 note 必须是 str 或 null: {ev!r}")
        elif kind == "r":
            if len(ev) != 3:
                raise CaptureError(f"[r,t,u] 字段数必须为 3: {ev!r}")
            _, _, u = ev
            if not isinstance(u, str) or not u:
                raise CaptureError(f"撤回目标 u 必须是非空域路径: {ev!r}")
            if u == _ROOT:
                raise CaptureError("根域不可撤回")
            if not self._domain_live(u):
                raise CaptureError(f"撤回目标域 {u!r} 不存在或已撤回: {ev!r}")
        elif kind == "c":
            if len(ev) != 2:
                raise CaptureError(f"[c,t] 字段数必须为 2: {ev!r}")

    def _tag_exists(self, tag: str) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM tags WHERE tag=?", (tag,)
        ).fetchone() is not None

    def _domain_known(self, path: str) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM domains WHERE path=?", (path,)
        ).fetchone() is not None

    def _domain_live(self, path: str) -> bool:
        row = self._conn.execute(
            "SELECT alive FROM domains WHERE path=?", (path,)
        ).fetchone()
        return row is not None and row[0] == 1

    # ------------------------------------------------------------------ #
    # 单事件事务应用（校验通过后才进入；失败回滚，位置不推进）
    # ------------------------------------------------------------------ #
    def _apply(self, gpos: int, local_pos: int, ev: list) -> None:
        kind = ev[0]
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO applied(gpos, local_pos, sid, ev) VALUES (?,?,?,?)",
                (gpos, local_pos, self._sid, json.dumps(ev, ensure_ascii=False)),
            )
            if kind in ("b", "s", "w", "r", "c"):
                conn.execute(
                    "INSERT INTO tags(tag, kind, gpos) VALUES (?,?,?)",
                    (ev[1], kind, gpos),
                )

            if kind == "b":
                path = _SEP + ev[1]
                conn.execute(
                    "INSERT INTO domains(path, parent, name, alive, born_gpos)"
                    " VALUES (?,?,?,1,?)",
                    (path, _ROOT, ev[1], gpos),
                )
            elif kind == "s":
                _, _, u, p = ev
                path = p + _SEP + u
                conn.execute(
                    "INSERT INTO domains(path, parent, name, alive, born_gpos)"
                    " VALUES (?,?,?,1,?)",
                    (path, p, u, gpos),
                )
            elif kind == "w":
                _, _, u, key, patch = ev
                for col in _COLUMNS:
                    if col in patch:
                        conn.execute(
                            "INSERT INTO patches(domain, key, col, val,"
                            " born_gpos, born_local) VALUES (?,?,?,?,?,?)",
                            (
                                u,
                                key,
                                col,
                                json.dumps(patch[col], ensure_ascii=False),
                                gpos,
                                local_pos,
                            ),
                        )
            elif kind == "r":
                u = ev[2]
                prefix = u + _SEP
                # 子树（含自身）全部置死
                conn.execute(
                    "UPDATE domains SET alive=0 WHERE path=? "
                    "OR substr(path,1,?)=?",
                    (u, len(prefix), prefix),
                )
                # 子树内全部幸存补丁随撤回失效
                conn.execute(
                    "UPDATE patches SET retract_gpos=? "
                    "WHERE retract_gpos IS NULL AND "
                    "(domain=? OR substr(domain,1,?)=?)",
                    (gpos, u, len(prefix), prefix),
                )
            elif kind == "c":
                snapshot = json.dumps(
                    self._snapshot(local_pos), ensure_ascii=False
                )
                conn.execute(
                    "INSERT INTO receipts(gpos, sid, local_pos, tag, content)"
                    " VALUES (?,?,?,?,?)",
                    (gpos, self._sid, local_pos, ev[1], snapshot),
                )

            # 消费位推进随事件同事务持久
            self._consumed = local_pos
            conn.execute(
                "UPDATE meta SET v=? WHERE k='consumed'", (str(local_pos),)
            )
            if kind == "a":
                self._ackreq = local_pos
                conn.execute(
                    "UPDATE meta SET v=? WHERE k='ackreq'", (str(local_pos),)
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------ #
    # 投影：seed 初值 + 截至某消费位的幸存补丁，按事件序叠加，按键排序
    # ------------------------------------------------------------------ #
    def _snapshot(self, upto_local: int) -> list:
        rows: dict[str, list] = {}
        for key, row in self._seed.items():
            rows[key] = [row["x"], row["y"], row["note"]]
        cur = self._conn.execute(
            "SELECT key, col, val FROM patches "
            "WHERE born_local <= ? AND retract_gpos IS NULL "
            "ORDER BY born_local, id",
            (upto_local,),
        )
        for key, col, val_json in cur:
            row = rows.setdefault(key, [0, 0, None])
            row[_COLUMNS.index(col)] = json.loads(val_json)
        return [[key, rows[key]] for key in sorted(rows)]

    def _set_meta(self, k: str, v: str) -> None:
        self._conn.execute("UPDATE meta SET v=? WHERE k=?", (v, k))
        self._conn.commit()

    def _check_open(self) -> None:
        if self._closed:
            raise CaptureError("入仓库已关闭")
