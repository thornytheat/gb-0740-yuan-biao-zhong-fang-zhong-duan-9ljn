import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

ACTIVE = "active"
HANDED_OVER = "handed_over"
EXPIRED = "expired"


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class LeaseStore:
    """交接租约的持久层。

    所有状态变更都在一个 IMMEDIATE 事务内完成，fencing token 由库内
    单调序列签发，旧执行者拿到的 token 永远无法再次通过守卫。
    """

    def __init__(self, path, clock=utc_now):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        with closing(self.connection()) as db, db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS fencing_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS leases (
                    lease_id TEXT PRIMARY KEY,
                    token INTEGER NOT NULL UNIQUE,
                    holder TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    scope_json TEXT NOT NULL,
                    graph_json TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL CHECK (status IN ('active','handed_over','expired')),
                    predecessor_id TEXT,
                    created_at TEXT NOT NULL,
                    renewed_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS lease_scope (
                    lease_id TEXT NOT NULL,
                    partition TEXT NOT NULL,
                    PRIMARY KEY (lease_id, partition)
                );
                CREATE TABLE IF NOT EXISTS handoffs (
                    handoff_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    lease_id TEXT NOT NULL,
                    from_group TEXT NOT NULL,
                    to_group TEXT NOT NULL,
                    to_holder TEXT NOT NULL DEFAULT '',
                    scope_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','completed','rejected','expired')),
                    reason TEXT NOT NULL DEFAULT '',
                    new_lease_id TEXT,
                    created_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS evidence (
                    evidence_hash TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS replay_requests (
                    request_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    partition TEXT NOT NULL,
                    target_version TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('waiting','dispatched','written','rolled_back','failed')
                    ),
                    evidence_hash TEXT,
                    lease_id TEXT,
                    detail TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS write_events (
                    event_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL,
                    token INTEGER NOT NULL,
                    partition TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK (kind IN ('write','rollback')),
                    previous_version TEXT,
                    version TEXT NOT NULL,
                    evidence_hash TEXT,
                    detail TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS partition_heads (
                    partition TEXT PRIMARY KEY,
                    version TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def connection(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        return db

    # ---------- fencing token ----------
    @staticmethod
    def next_token(db):
        """库内单调递增的 fencing token，交接只会让令牌变大。"""
        db.execute(
            "INSERT INTO fencing_meta(key, value) VALUES ('token', 1) "
            "ON CONFLICT(key) DO UPDATE SET value = value + 1"
        )
        return db.execute("SELECT value FROM fencing_meta WHERE key = 'token'").fetchone()[0]

    # ---------- leases ----------
    def active_lease_on(self, db, partition):
        row = db.execute(
            "SELECT l.* FROM leases l JOIN lease_scope s ON s.lease_id = l.lease_id "
            "WHERE s.partition = ? AND l.status = 'active'",
            (partition,),
        ).fetchone()
        return dict(row) if row else None

    def active_partitions(self, db, partitions):
        if not partitions:
            return {}
        marks = ",".join("?" for _ in partitions)
        rows = db.execute(
            f"SELECT DISTINCT s.partition, l.lease_id FROM lease_scope s "
            f"JOIN leases l ON l.lease_id = s.lease_id "
            f"WHERE l.status = 'active' AND s.partition IN ({marks})",
            tuple(partitions),
        ).fetchall()
        return {row["partition"]: row["lease_id"] for row in rows}

    def get_lease(self, lease_id):
        with closing(self.connection()) as db:
            row = db.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
            return dict(row) if row else None

    def list_leases(self, limit=50):
        with closing(self.connection()) as db:
            rows = db.execute("SELECT * FROM leases ORDER BY token DESC LIMIT ?", (limit,)).fetchall()
            return [dict(row) for row in rows]

    def insert_lease(self, db, lease_id, holder, group_name, scope, graph, note,
                     predecessor_id, ttl_seconds):
        token = self.next_token(db)
        now = self.clock()
        expires = datetime.fromisoformat(now).timestamp() + ttl_seconds
        expires_at = datetime.fromtimestamp(expires, timezone.utc).isoformat(timespec="seconds")
        db.execute(
            "INSERT INTO leases VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                lease_id, token, holder, group_name, json.dumps(scope, ensure_ascii=False),
                json.dumps(graph, ensure_ascii=False), note, ACTIVE, predecessor_id,
                now, now, expires_at,
            ),
        )
        db.executemany(
            "INSERT INTO lease_scope(lease_id, partition) VALUES (?,?)",
            [(lease_id, part) for part in scope],
        )
        return token, now, expires_at

    def mark_lease(self, db, lease_id, status):
        db.execute(
            "UPDATE leases SET status = ? WHERE lease_id = ?", (status, lease_id)
        )

    def expire_due(self, db, now_iso):
        db.execute(
            "UPDATE leases SET status = 'expired' WHERE status = 'active' AND expires_at <= ?",
            (now_iso,),
        )
        db.execute(
            "UPDATE handoffs SET status = 'expired', finished_at = ? "
            "WHERE status = 'pending' AND created_at <= ?",
            (now_iso, now_iso),
        )

    def sweep(self):
        with closing(self.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            self.expire_due(db, self.clock())

    # ---------- handoffs ----------
    def get_handoff(self, handoff_id=None, request_id=None):
        with closing(self.connection()) as db:
            if handoff_id:
                row = db.execute("SELECT * FROM handoffs WHERE handoff_id = ?", (handoff_id,)).fetchone()
            else:
                row = db.execute("SELECT * FROM handoffs WHERE request_id = ?", (request_id,)).fetchone()
            return dict(row) if row else None

    def list_handoffs(self, limit=50):
        with closing(self.connection()) as db:
            rows = db.execute(
                "SELECT * FROM handoffs ORDER BY rowid DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(row) for row in rows]

    def insert_handoff(self, db, handoff_id, request_id, lease_id, from_group,
                       to_group, to_holder, scope, reason, now):
        db.execute(
            "INSERT INTO handoffs(handoff_id, request_id, lease_id, from_group, to_group, "
            "to_holder, scope_json, status, reason, created_at) "
            "VALUES (?,?,?,?,?,?,?, 'pending', ?, ?)",
            (handoff_id, request_id, lease_id, from_group, to_group, to_holder,
             json.dumps(scope, ensure_ascii=False), reason, now),
        )

    def finish_handoff(self, db, handoff_id, status, new_lease_id, reason, now):
        db.execute(
            "UPDATE handoffs SET status = ?, new_lease_id = ?, reason = ?, finished_at = ? "
            "WHERE handoff_id = ?",
            (status, new_lease_id, reason, now, handoff_id),
        )

    # ---------- evidence (append-only) ----------
    def get_evidence(self, evidence_hash):
        with closing(self.connection()) as db:
            row = db.execute(
                "SELECT evidence_hash, payload_json, created_at FROM evidence WHERE evidence_hash = ?",
                (evidence_hash,),
            ).fetchone()
            return dict(row) if row else None

    def put_evidence(self, db, evidence_hash, payload):
        """只在调用方连接内写入；返回 True 表示新增，False 表示既有。"""
        cur = db.execute(
            "INSERT OR IGNORE INTO evidence VALUES (?,?,?)",
            (evidence_hash, json.dumps(payload, ensure_ascii=False), self.clock()),
        )
        return cur.rowcount > 0

    # ---------- replay requests ----------
    def get_request(self, request_id=None, idempotency_key=None):
        with closing(self.connection()) as db:
            if request_id:
                row = db.execute("SELECT * FROM replay_requests WHERE request_id = ?", (request_id,)).fetchone()
            else:
                row = db.execute(
                    "SELECT * FROM replay_requests WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
            return dict(row) if row else None

    def list_requests(self, status=None, limit=200):
        with closing(self.connection()) as db:
            if status:
                rows = db.execute(
                    "SELECT * FROM replay_requests WHERE status = ? ORDER BY rowid DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT * FROM replay_requests ORDER BY rowid DESC LIMIT ?", (limit,)
                ).fetchall()
            return [dict(row) for row in rows]

    def insert_request(self, db, req):
        db.execute(
            "INSERT INTO replay_requests(request_id, idempotency_key, partition, target_version, "
            "status, evidence_hash, lease_id, detail, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                req["request_id"], req["idempotency_key"], req["partition"],
                req["target_version"], req["status"], req.get("evidence_hash"),
                req.get("lease_id"), req.get("detail", ""), req["created_at"], req["updated_at"],
            ),
        )

    def update_request(self, db, request_id, status, lease_id, detail=""):
        db.execute(
            "UPDATE replay_requests SET status = ?, lease_id = ?, detail = ?, updated_at = ? "
            "WHERE request_id = ?",
            (status, lease_id, detail, self.clock(), request_id),
        )

    # ---------- heads / events ----------
    def head(self, db, partition):
        row = db.execute(
            "SELECT version FROM partition_heads WHERE partition = ?", (partition,)
        ).fetchone()
        return row["version"] if row else None

    def list_heads(self):
        with closing(self.connection()) as db:
            return {row["partition"]: row["version"] for row in
                    db.execute("SELECT partition, version FROM partition_heads")}

    def put_head(self, db, partition, version, now):
        db.execute(
            "INSERT INTO partition_heads VALUES (?,?,?) "
            "ON CONFLICT(partition) DO UPDATE SET version = excluded.version, updated_at = excluded.updated_at",
            (partition, version, now),
        )

    def delete_head(self, db, partition):
        db.execute("DELETE FROM partition_heads WHERE partition = ?", (partition,))

    def insert_event(self, db, event):
        db.execute(
            "INSERT INTO write_events(event_id, request_id, lease_id, token, partition, kind, "
            "previous_version, version, evidence_hash, detail, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                event["event_id"], event["request_id"], event["lease_id"], event["token"],
                event["partition"], event["kind"], event.get("previous_version"),
                event["version"], event.get("evidence_hash"), event.get("detail", ""),
                event["created_at"],
            ),
        )

    def list_events(self, limit=200):
        with closing(self.connection()) as db:
            rows = db.execute(
                "SELECT * FROM write_events ORDER BY rowid DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(row) for row in rows]
