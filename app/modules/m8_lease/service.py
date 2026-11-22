"""重放执行租约服务。

派发、写入和回滚都依据租约携带的 fencing token：库内单调递增，交接后
旧租约立即失效，旧执行者继续提交时守卫拒绝，因此无法落库。同一请求
以幂等键去重，重试不产生第二份；证据只追加，原始依据被误删后仍可
复核当时为何放行。
"""
import hashlib
import json
import uuid
from contextlib import closing

from app.keys import ReplayError
from app.modules.m8_lease.graph import closure, dataset_of
from app.modules.m8_lease.store import ACTIVE, HANDED_OVER, LeaseStore

DEFAULT_TTL_SECONDS = 4 * 3600


def _hash(payload):
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _new_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _validate_partitions(partitions, datasets):
    if not isinstance(partitions, list) or not partitions:
        raise ReplayError("SCOPE_EMPTY", "租约范围至少包含一个分区")
    bad = [p for p in partitions if not isinstance(p, str) or "@" not in p
           or dataset_of(p) not in datasets]
    if bad:
        raise ReplayError("PARTITION_UNKNOWN", f"分区未登记: {bad[0]}")
    return sorted(set(partitions))


def _text(value, field, limit=120):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ReplayError("FIELD", f"{field} 需为 1–{limit} 字的非空文本")
    return value.strip()


class LeaseService:
    def __init__(self, store: LeaseStore):
        self.store = store

    # ================= 租约获取 =================
    def acquire(self, partitions, holder, group_name, datasets, note="", ttl_seconds=DEFAULT_TTL_SECONDS):
        """为分区的依赖闭包获取一份活动租约。

        闭包内任何分区已有活动租约都视为范围冲突——避免把相互依赖的
        分区拆给不同持有者。互不相连的分区可并行由不同组持有。
        """
        scope, related = closure(_validate_partitions(partitions, datasets), datasets)
        holder = _text(holder, "执行者")
        group_name = _text(group_name, "值班组")
        if not isinstance(note, str) or len(note) > 400:
            raise ReplayError("FIELD", "备注过长")
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 7 * 24 * 3600:
            raise ReplayError("FIELD", "TTL 须为 1 秒至 7 天的整数秒")
        lease_id = _new_id("lease")
        graph_snapshot = {
            "datasets": sorted(related),
            "edges": [list(edge) for edge in _edges_of(datasets, related)],
        }
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            self.store.expire_due(db, self.store.clock())
            conflicts = self.store.active_partitions(db, scope)
            if conflicts:
                part = sorted(conflicts)[0]
                raise ReplayError(
                    "SCOPE_CONFLICT",
                    f"分区 {part} 已被活动租约 {conflicts[part]} 持有，依赖链不可拆分",
                )
            token, now, expires_at = self.store.insert_lease(
                db, lease_id, holder, group_name, scope, graph_snapshot, note, None, ttl_seconds
            )
        return self.lease_view(lease_id)

    # ================= 两阶段交接 =================
    def begin_handoff(self, lease_id, to_group, to_holder, reason=""):
        """旧组发起交接：生成待接收交接单，租约仍由旧组持有。"""
        to_group = _text(to_group, "接手值班组")
        to_holder = _text(to_holder, "接手执行者")
        if not isinstance(reason, str) or len(reason) > 400:
            raise ReplayError("FIELD", "交接说明过长")
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            lease = self._load_active_lease(db, lease_id)
            handoff_id = _new_id("ho")
            request_id = _new_id("hor")
            scope = json.loads(lease["scope_json"])
            self.store.insert_handoff(
                db, handoff_id, request_id, lease_id, lease["group_name"],
                to_group, to_holder, scope, reason, self.store.clock(),
            )
        return self.handoff_view(handoff_id)

    def complete_handoff(self, handoff_id, to_holder):
        """接手组确认：同一事务内旧租约作废、新租约拿到更高 token。

        失败时不删单据，标记 rejected/expired，旧范围仍可追踪。
        """
        to_holder = _text(to_holder, "接手执行者")
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM handoffs WHERE handoff_id = ?", (handoff_id,)).fetchone()
            if not row:
                raise ReplayError("HANDOFF_NOT_FOUND", "交接单不存在")
            handoff = dict(row)
            if handoff["status"] != "pending":
                raise ReplayError("HANDOFF_STATE", f"交接单已 {handoff['status']}，不可再确认")
            old = db.execute("SELECT * FROM leases WHERE lease_id = ?", (handoff["lease_id"],)).fetchone()
            old = dict(old) if old else None
            if not old or old["status"] != ACTIVE:
                self.store.finish_handoff(
                    db, handoff_id, "rejected", None, "旧租约已不在活动态", self.store.clock()
                )
                raise ReplayError("HANDOFF_STATE", "旧租约已失效，交接被拒绝，旧范围仍保留在交接单中")
            scope = json.loads(handoff["scope_json"])
            conflicts = {p: lid for p, lid in self.store.active_partitions(db, scope).items()
                         if lid != old["lease_id"]}
            if conflicts:
                part = sorted(conflicts)[0]
                self.store.finish_handoff(
                    db, handoff_id, "rejected", None,
                    f"范围冲突: {part} -> {conflicts[part]}", self.store.clock(),
                )
                raise ReplayError("SCOPE_CONFLICT", f"交接范围与租约 {conflicts[part]} 冲突")
            new_id = _new_id("lease")
            token, now, expires_at = self.store.insert_lease(
                db, new_id, to_holder, handoff["to_group"], scope,
                json.loads(old["graph_json"]),
                f"交接自 {old['lease_id']}（{old['group_name']} → {handoff['to_group']}）",
                old["lease_id"], DEFAULT_TTL_SECONDS,
            )
            self.store.mark_lease(db, old["lease_id"], HANDED_OVER)
            self.store.finish_handoff(db, handoff_id, "completed", new_id, "", now)
            db.execute(
                "UPDATE replay_requests SET lease_id = ?, updated_at = ? "
                "WHERE lease_id = ? AND status IN ('waiting','dispatched')",
                (new_id, now, old["lease_id"]),
            )
        return self.lease_view(new_id)

    def reject_handoff(self, handoff_id, reason):
        reason = _text(reason, "拒绝原因")
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM handoffs WHERE handoff_id = ?", (handoff_id,)).fetchone()
            if not row:
                raise ReplayError("HANDOFF_NOT_FOUND", "交接单不存在")
            if dict(row)["status"] != "pending":
                raise ReplayError("HANDOFF_STATE", "交接单已结束")
            self.store.finish_handoff(db, handoff_id, "rejected", None, reason, self.store.clock())
        return self.handoff_view(handoff_id)

    def release_lease(self, lease_id):
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            lease = self._load_active_lease(db, lease_id)
            self.store.mark_lease(db, lease_id, "expired")
        return self.lease_view(lease_id)

    # ================= 证据（只追加） =================
    def add_evidence(self, payload):
        if not isinstance(payload, dict) or not payload:
            raise ReplayError("EVIDENCE", "证据须是非空对象")
        body = dict(payload)
        body.setdefault("captured_at", self.store.clock())
        evidence_hash = _hash(body)
        with closing(self.store.connection()) as db, db:
            self.store.put_evidence(db, evidence_hash, body)
        return evidence_hash

    def get_evidence(self, evidence_hash):
        record = self.store.get_evidence(evidence_hash)
        if not record:
            raise ReplayError("EVIDENCE_NOT_FOUND", "证据不存在或已被移除，无法复核放行依据")
        return record

    # ================= 请求登记（幂等） =================
    def submit_request(self, partition, target_version, evidence_payload=None,
                       idempotency_key=None, detail=""):
        if not isinstance(partition, str) or "@" not in partition:
            raise ReplayError("PARTITION_UNKNOWN", "分区格式应为 dataset@YYYY-MM-DD")
        if not isinstance(target_version, str) or not target_version.strip():
            raise ReplayError("FIELD", "目标版本不能为空")
        evidence_hash = None
        if evidence_payload is not None:
            evidence_hash = self.add_evidence(evidence_payload)
        if not idempotency_key:
            idempotency_key = _hash(
                {"partition": partition, "target_version": target_version,
                 "evidence": evidence_hash, "kind": "replay-request"}
            )
        elif not isinstance(idempotency_key, str) or len(idempotency_key) > 120:
            raise ReplayError("FIELD", "幂等键格式无效")
        existing = self.store.get_request(idempotency_key=idempotency_key)
        if existing:
            view = self.request_view(existing)
            view["reused"] = True
            return view
        now = self.store.clock()
        request_id = _new_id("req")
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            owner = self.store.active_lease_on(db, partition)
            req = {
                "request_id": request_id,
                "idempotency_key": idempotency_key,
                "partition": partition,
                "target_version": target_version.strip(),
                "status": "waiting",
                "evidence_hash": evidence_hash,
                "lease_id": owner["lease_id"] if owner else None,
                "detail": detail if isinstance(detail, str) else "",
                "created_at": now,
                "updated_at": now,
            }
            try:
                self.store.insert_request(db, req)
            except Exception as exc:  # 并发下唯一键竞争：复用先到者
                found = self.store.get_request(idempotency_key=idempotency_key)
                if found:
                    view = self.request_view(found)
                    view["reused"] = True
                    return view
                raise ReplayError("IDEMPOTENCY", "请求登记冲突") from exc
        view = self.request_view(self.store.get_request(request_id))
        view["reused"] = False
        return view

    # ================= 守卫：派发 / 写入 / 回滚 =================
    def _expire_if_needed(self, lease_id):
        """在写事务之外先把到期租约落库，避免随拒绝异常一起回滚。"""
        lease = self.store.get_lease(lease_id)
        if lease and lease["status"] == ACTIVE and lease["expires_at"] <= self.store.clock():
            with closing(self.store.connection()) as db, db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT status FROM leases WHERE lease_id = ?", (lease_id,)
                ).fetchone()
                if dict(row)["status"] == ACTIVE:
                    self.store.mark_lease(db, lease_id, "expired")
            return True
        return False

    def _guard(self, db, lease_id, token, partition):
        lease = db.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
        if not lease:
            raise ReplayError("LEASE_NOT_FOUND", "租约不存在")
        lease = dict(lease)
        if lease["status"] != ACTIVE or lease["expires_at"] <= self.store.clock():
            raise ReplayError(
                "LEASE_EXPIRED",
                f"租约 {lease_id} 已失效（{lease['status']}），旧执行者不能再落库",
            )
        if int(token) != lease["token"]:
            raise ReplayError(
                "STALE_FENCING_TOKEN",
                f"token {token} 与当前持有者 token {lease['token']} 不符，拒绝落库",
            )
        scoped = db.execute(
            "SELECT 1 FROM lease_scope WHERE lease_id = ? AND partition = ?",
            (lease_id, partition),
        ).fetchone()
        if not scoped:
            raise ReplayError("NOT_HOLDER", f"分区 {partition} 不在租约 {lease_id} 范围内")
        return lease

    def dispatch(self, request_id, lease_id, token):
        """派发：只有当前持有者、持当前 token 才能把等待请求纳入执行。"""
        self._expire_if_needed(lease_id)
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            req = self._load_request(db, request_id)
            self._guard(db, lease_id, token, req["partition"])
            if req["status"] not in ("waiting", "dispatched", "failed"):
                raise ReplayError("REQUEST_STATE", f"请求处于 {req['status']}，不可派发")
            self.store.update_request(db, request_id, "dispatched", lease_id, "已派发")
        return self.request_view(self.store.get_request(request_id))

    def write(self, request_id, lease_id, token):
        """写入：fencing 守卫 + 指针陈旧检查，通过后落库并记录写事件。"""
        self._expire_if_needed(lease_id)
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            req = self._load_request(db, request_id)
            lease = self._guard(db, lease_id, token, req["partition"])
            if req["status"] not in ("dispatched", "failed"):
                raise ReplayError("REQUEST_STATE", f"请求处于 {req['status']}，须先派发")
            self._check_evidence(db, req)
            previous = self.store.head(db, req["partition"])
            now = self.store.clock()
            self.store.put_head(db, req["partition"], req["target_version"], now)
            self.store.insert_event(db, {
                "event_id": _new_id("evt"),
                "request_id": request_id,
                "lease_id": lease_id,
                "token": lease["token"],
                "partition": req["partition"],
                "kind": "write",
                "previous_version": previous,
                "version": req["target_version"],
                "evidence_hash": req["evidence_hash"],
                "detail": req["detail"],
                "created_at": now,
            })
            self.store.update_request(db, request_id, "written", lease_id, f"自 {previous} 写入")
        return self.request_view(self.store.get_request(request_id))

    def rollback(self, request_id, lease_id, token):
        """回滚：同一守卫；先核验当前版本仍是本租约写入的版本，再恢复旧指针。"""
        self._expire_if_needed(lease_id)
        with closing(self.store.connection()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            req = self._load_request(db, request_id)
            lease = self._guard(db, lease_id, token, req["partition"])
            if req["status"] != "written":
                raise ReplayError("REQUEST_STATE", "仅已写入请求可回滚")
            current = self.store.head(db, req["partition"])
            if current != req["target_version"]:
                raise ReplayError(
                    "STALE_ROLLBACK",
                    f"{req['partition']} 当前版本 {current} 已被替换，拒绝回滚",
                )
            event = db.execute(
                "SELECT * FROM write_events WHERE request_id = ? AND kind = 'write' "
                "ORDER BY rowid DESC LIMIT 1",
                (request_id,),
            ).fetchone()
            previous = dict(event)["previous_version"] if event else None
            now = self.store.clock()
            if previous is None:
                self.store.delete_head(db, req["partition"])
            else:
                self.store.put_head(db, req["partition"], previous, now)
            self.store.insert_event(db, {
                "event_id": _new_id("evt"),
                "request_id": request_id,
                "lease_id": lease_id,
                "token": lease["token"],
                "partition": req["partition"],
                "kind": "rollback",
                "previous_version": current,
                "version": previous or "",
                "evidence_hash": req["evidence_hash"],
                "detail": f"回滚至 {previous}",
                "created_at": now,
            })
            self.store.update_request(db, request_id, "rolled_back", lease_id, f"恢复至 {previous}")
        return self.request_view(self.store.get_request(request_id))

    # ================= 读取视图 =================
    def lease_view(self, lease_id):
        lease = self.store.get_lease(lease_id)
        if not lease:
            raise ReplayError("LEASE_NOT_FOUND", "租约不存在")
        with closing(self.store.connection()) as db:
            scope = [r["partition"] for r in db.execute(
                "SELECT partition FROM lease_scope WHERE lease_id = ? ORDER BY partition",
                (lease_id,),
            )]
        return {
            "租约": lease["lease_id"],
            "fencing令牌": lease["token"],
            "持有者": lease["holder"],
            "值班组": lease["group_name"],
            "状态": lease["status"],
            "责任范围": scope,
            "范围大小": len(scope),
            "依赖图快照": json.loads(lease["graph_json"]),
            "前序租约": lease["predecessor_id"],
            "备注": lease["note"],
            "创建时间": lease["created_at"],
            "到期时间": lease["expires_at"],
        }

    def handoff_view(self, handoff_id):
        handoff = self.store.get_handoff(handoff_id)
        if not handoff:
            raise ReplayError("HANDOFF_NOT_FOUND", "交接单不存在")
        return {
            "交接单": handoff["handoff_id"],
            "请求号": handoff["request_id"],
            "源租约": handoff["lease_id"],
            "交出组": handoff["from_group"],
            "接手组": handoff["to_group"],
            "接手执行者": handoff["to_holder"],
            "交接范围": json.loads(handoff["scope_json"]),
            "状态": handoff["status"],
            "新租约": handoff["new_lease_id"],
            "说明": handoff["reason"],
            "创建时间": handoff["created_at"],
            "完成时间": handoff["finished_at"],
        }

    def request_view(self, row):
        return {
            "请求": row["request_id"],
            "幂等键": row["idempotency_key"],
            "分区": row["partition"],
            "目标版本": row["target_version"],
            "状态": row["status"],
            "归属租约": row["lease_id"],
            "证据摘要": row["evidence_hash"],
            "明细": row["detail"],
            "创建时间": row["created_at"],
            "更新时间": row["updated_at"],
        }

    def waiting_view(self):
        """页面等待范围：等待/派发中的请求，连同当前持有者与分区指针。"""
        requests = [r for r in self.store.list_requests() if r["status"] in ("waiting", "dispatched")]
        leases = {row["lease_id"]: self.store.get_lease(row["lease_id"])
                  for row in requests if row["lease_id"]}
        heads = self.store.list_heads()
        return [{
            **self.request_view(row),
            "当前版本": heads.get(row["partition"]),
            "持有组": (leases.get(row["lease_id"]) or {}).get("group_name") if row["lease_id"] else None,
            "fencing令牌": (leases.get(row["lease_id"]) or {}).get("token") if row["lease_id"] else None,
        } for row in requests]

    def dashboard(self, datasets):
        self.store.sweep()
        leases = [self.lease_view(row["lease_id"]) for row in self.store.list_leases()]
        handoffs = [self.handoff_view(row["handoff_id"]) for row in self.store.list_handoffs()]
        return {
            "租约列表": leases,
            "交接单": handoffs,
            "等待范围": self.waiting_view(),
            "分区指针": self.store.list_heads(),
            "写事件": [dict(r) for r in self.store.list_events()],
            "分量图": _cluster_view(datasets),
        }

    # ================= 内部辅助 =================
    def _load_active_lease(self, db, lease_id):
        row = db.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
        if not row:
            raise ReplayError("LEASE_NOT_FOUND", "租约不存在")
        lease = dict(row)
        if lease["status"] != ACTIVE or lease["expires_at"] <= self.store.clock():
            raise ReplayError("LEASE_EXPIRED", f"租约 {lease_id} 已 {lease['status']}")
        return lease

    @staticmethod
    def _load_request(db, request_id):
        row = db.execute("SELECT * FROM replay_requests WHERE request_id = ?", (request_id,)).fetchone()
        if not row:
            raise ReplayError("REQUEST_NOT_FOUND", "重放请求不存在")
        return dict(row)

    def _check_evidence(self, db, req):
        if not req["evidence_hash"]:
            return
        row = db.execute(
            "SELECT 1 FROM evidence WHERE evidence_hash = ?", (req["evidence_hash"],)
        ).fetchone()
        if not row:
            raise ReplayError("EVIDENCE_NOT_FOUND", "放行依据缺失，拒绝写入")


def _edges_of(datasets, related):
    from app.modules.m8_lease.graph import undirected_edges
    related = set(related)
    return [edge for edge in undirected_edges(datasets) if edge[0] in related and edge[1] in related]


def _cluster_view(datasets):
    from app.modules.m8_lease.graph import cluster_map
    membership = cluster_map(datasets)
    groups = {}
    for name, root in sorted(membership.items()):
        groups.setdefault(root, []).append(name)
    return [{"分量": root, "数据集": sorted(names)} for root, names in sorted(groups.items())]
