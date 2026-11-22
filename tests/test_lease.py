import contextlib

import pytest

from app.keys import ReplayError
from app.main import create_app
from app.modules.m2_graph.registry import catalog as build_catalog
from app.modules.m8_lease.graph import closure, cluster_map, undirected_edges
from app.modules.m8_lease.service import LeaseService
from app.modules.m8_lease.store import LeaseStore
from app.modules.m1_inventory.reader import read_bundle
from app.config import ROOT


@contextlib.contextmanager
def expect_code(*codes):
    with pytest.raises(ReplayError) as exc:
        yield
    assert exc.value.code in codes, f"期望错误码 {codes}，实际 {exc.value.code}"


# ---------- 共用夹具 ----------
@pytest.fixture
def datasets(night):
    return build_catalog(read_bundle(night)["datasets"])


@pytest.fixture
def split_catalog():
    # 两个互不相连的分量：orders→facts 链；孤立表 metrics
    return build_catalog([
        {"id": "orders", "revision": "r1", "inputs": []},
        {"id": "facts", "revision": "r1",
         "inputs": [{"dataset": "orders", "lag": 0}]},
        {"id": "metrics", "revision": "r1", "inputs": []},
    ])


@pytest.fixture
def store(tmp_path):
    return LeaseStore(tmp_path / "lease.sqlite")


@pytest.fixture
def svc(store):
    return LeaseService(store)


@pytest.fixture
def http(tmp_path):
    return create_app({"TESTING": True, "DB": tmp_path / "plans.sqlite",
                       "LEASE_DB": tmp_path / "lease.sqlite"}).test_client()


def nightly_catalog(http_client):
    raw = (ROOT / "sample_data" / "nightly.json").read_bytes()
    return build_catalog(read_bundle(raw)["datasets"])


# ---------- 依赖闭包：相连分区同租约，无关分量可并行 ----------
class TestGraphClosure:
    def test_dependent_datasets_stay_in_one_component(self, datasets):
        # facts 读取 orders，二者必须落在同一分量
        edges = undirected_edges(datasets)
        assert ("facts", "orders") in edges
        mapping = cluster_map(datasets)
        assert mapping["orders"] == mapping["facts"]

    def test_closure_expands_to_full_dependency_chain(self, datasets):
        scope, related = closure(["orders@2026-11-17"], datasets)
        assert "orders@2026-11-17" in scope
        assert "facts@2026-11-17" in scope
        assert {"orders", "facts"}.issubset(set(related))

    def test_independent_components_can_be_parallel(self, split_catalog):
        mapping = cluster_map(split_catalog)
        roots = set(mapping.values())
        # metrics 与 orders→facts 链互不相连，可并行持有
        assert len(roots) == 2
        assert mapping["metrics"] != mapping["orders"]

    def test_closure_of_isolated_node_does_not_pull_chain(self, split_catalog):
        scope, related = closure(["metrics@2026-11-17"], split_catalog)
        assert scope == ["metrics@2026-11-17"]
        assert related == ["metrics"]


# ---------- 获取租约 ----------
class TestAcquire:
    def test_acquire_returns_monotonic_fencing_token(self, svc, split_catalog):
        first = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", split_catalog)
        second = svc.acquire(["metrics@2026-11-17"], "乙", "夜班B组", split_catalog)
        assert first["fencing令牌"] >= 1
        assert second["fencing令牌"] > first["fencing令牌"]
        assert first["状态"] == "active"

    def test_independent_scopes_can_be_held_in_parallel(self, svc, split_catalog):
        a = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", split_catalog)
        b = svc.acquire(["metrics@2026-11-17"], "乙", "夜班B组", split_catalog)
        assert a["租约"] != b["租约"] and b["状态"] == "active"

    def test_conflict_when_chain_partition_already_held(self, svc, datasets):
        svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        # facts 与 orders 同链，不可拆给另一组
        with expect_code("SCOPE_CONFLICT"):
            svc.acquire(["facts@2026-11-17"], "乙", "夜班B组", datasets)

    def test_unknown_partition_rejected(self, svc, datasets):
        with expect_code("PARTITION_UNKNOWN"):
            svc.acquire(["ghost@2026-11-17"], "甲", "夜班A组", datasets)

    def test_scope_includes_closure_not_just_named_partition(self, svc, datasets):
        view = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        names = {p.split("@")[0] for p in view["责任范围"]}
        assert "facts" in names and "orders" in names


# ---------- 请求幂等与等待范围 ----------
class TestRequests:
    def test_retry_same_idempotency_key_does_not_duplicate(self, svc, datasets):
        svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        payload = {"source": "nightly.json", "reason": "上游快照过期", "rows": 12}
        first = svc.submit_request("orders@2026-11-17", "s2", payload,
                                   idempotency_key="key-001")
        second = svc.submit_request("orders@2026-11-17", "s2", payload,
                                    idempotency_key="key-001")
        assert first["请求"] == second["请求"]
        assert second["reused"] is True
        waiting = svc.waiting_view()
        assert len([w for w in waiting if w["幂等键"] == "key-001"]) == 1

    def test_auto_idempotency_key_is_stable_for_same_intent(self, svc, datasets):
        a = svc.submit_request("orders@2026-11-17", "s9", {"v": 1})
        b = svc.submit_request("orders@2026-11-17", "s9", {"v": 1})
        assert a["请求"] == b["请求"] and b["reused"] is True

    def test_waiting_request_binds_to_existing_holder(self, svc, datasets):
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        req = svc.submit_request("orders@2026-11-17", "s2", {"r": 1})
        assert req["归属租约"] == lease["租约"]
        view = svc.waiting_view()[0]
        assert view["持有组"] == "夜班A组"
        assert view["fencing令牌"] == lease["fencing令牌"]

    def test_request_without_holder_still_waits(self, svc, split_catalog):
        req = svc.submit_request("metrics@2026-11-17", "v1", {"r": 1})
        assert req["状态"] == "waiting" and req["归属租约"] is None


# ---------- fencing：派发/写入/回滚守卫 ----------
class TestFencing:
    def _lease_and_request(self, svc, datasets):
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        req = svc.submit_request("orders@2026-11-17", "s2", {"source": "nightly.json"})
        return lease, req

    def test_dispatch_write_rollback_happy_path(self, svc, datasets):
        lease, req = self._lease_and_request(svc, datasets)
        lid, token = lease["租约"], lease["fencing令牌"]
        assert svc.dispatch(req["请求"], lid, token)["状态"] == "dispatched"
        written = svc.write(req["请求"], lid, token)
        assert written["状态"] == "written"
        assert svc.store.list_heads()["orders@2026-11-17"] == "s2"
        rolled = svc.rollback(req["请求"], lid, token)
        assert rolled["状态"] == "rolled_back"
        assert "orders@2026-11-17" not in svc.store.list_heads()

    def test_wrong_token_cannot_write(self, svc, datasets):
        lease, req = self._lease_and_request(svc, datasets)
        svc.dispatch(req["请求"], lease["租约"], lease["fencing令牌"])
        with expect_code("STALE_FENCING_TOKEN"):
            svc.write(req["请求"], lease["租约"], lease["fencing令牌"] + 999)
        # 被拒绝后没有落库
        assert "orders@2026-11-17" not in svc.store.list_heads()

    def test_old_executor_blocked_after_handoff(self, svc, datasets):
        lease, req = self._lease_and_request(svc, datasets)
        svc.dispatch(req["请求"], lease["租约"], lease["fencing令牌"])
        ho = svc.begin_handoff(lease["租约"], "夜班B组", "乙", "夜班交接")
        new_lease = svc.complete_handoff(ho["交接单"], "乙")
        # 旧执行者拿着旧 token 继续写 → 拒绝
        with expect_code("LEASE_EXPIRED", "STALE_FENCING_TOKEN"):
            svc.write(req["请求"], lease["租约"], lease["fencing令牌"])
        # 新持有者用新 token 可以写
        svc.dispatch(req["请求"], new_lease["租约"], new_lease["fencing令牌"])
        ok = svc.write(req["请求"], new_lease["租约"], new_lease["fencing令牌"])
        assert ok["状态"] == "written"
        assert svc.store.list_heads()["orders@2026-11-17"] == "s2"

    def test_non_holder_cannot_dispatch(self, svc, split_catalog):
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", split_catalog)
        req = svc.submit_request("orders@2026-11-17", "s2", {"r": 1})
        other = svc.acquire(["metrics@2026-11-17"], "乙", "夜班B组", split_catalog)
        with expect_code("NOT_HOLDER"):
            svc.dispatch(req["请求"], other["租约"], other["fencing令牌"])

    def test_write_requires_dispatch_first(self, svc, datasets):
        lease, req = self._lease_and_request(svc, datasets)
        with expect_code("REQUEST_STATE"):
            svc.write(req["请求"], lease["租约"], lease["fencing令牌"])

    def test_rollback_rejects_when_version_replaced(self, svc, datasets):
        lease, req = self._lease_and_request(svc, datasets)
        lid, token = lease["租约"], lease["fencing令牌"]
        svc.dispatch(req["请求"], lid, token)
        svc.write(req["请求"], lid, token)
        # 外部把指针改成别的版本
        svc.store.list_heads  # 仅示意；直接改库模拟越界替换
        with svc.store.connection() as db, db:
            db.execute("UPDATE partition_heads SET version='sX' WHERE partition=?",
                       ("orders@2026-11-17",))
        with expect_code("STALE_ROLLBACK"):
            svc.rollback(req["请求"], lid, token)


# ---------- 两阶段交接 ----------
class TestHandoff:
    def test_handoff_keeps_old_scope_trackable_and_rotates_token(self, svc, datasets):
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        ho = svc.begin_handoff(lease["租约"], "夜班B组", "乙", "换班")
        assert ho["状态"] == "pending"
        # 待接收期间旧租约仍活动
        assert svc.lease_view(lease["租约"])["状态"] == "active"
        new_lease = svc.complete_handoff(ho["交接单"], "乙")
        assert new_lease["fencing令牌"] > lease["fencing令牌"]
        assert new_lease["责任范围"] == svc.lease_view(lease["租约"])["责任范围"]
        assert svc.lease_view(lease["租约"])["状态"] == "handed_over"
        record = svc.handoff_view(ho["交接单"])
        assert record["状态"] == "completed" and record["新租约"] == new_lease["租约"]

    def test_rejected_handoff_keeps_old_lease_and_audit_trail(self, svc, datasets):
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        ho = svc.begin_handoff(lease["租约"], "夜班B组", "乙")
        svc.reject_handoff(ho["交接单"], "交接凭据不全")
        assert svc.lease_view(lease["租约"])["状态"] == "active"
        record = svc.handoff_view(ho["交接单"])
        assert record["状态"] == "rejected" and "凭据不全" in record["说明"]
        # 再次确认一张已结束的交接单会被拒
        with expect_code("HANDOFF_STATE"):
            svc.complete_handoff(ho["交接单"], "乙")

    def test_waiting_requests_follow_new_lease_on_handoff(self, svc, datasets):
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        req = svc.submit_request("orders@2026-11-17", "s2", {"r": 1})
        ho = svc.begin_handoff(lease["租约"], "夜班B组", "乙")
        new_lease = svc.complete_handoff(ho["交接单"], "乙")
        refreshed = svc.store.get_request(request_id=req["请求"])
        assert refreshed["lease_id"] == new_lease["租约"]

    def test_handoff_unknown_lease_rejected(self, svc, datasets):
        with expect_code("LEASE_NOT_FOUND"):
            svc.begin_handoff("lease_missing", "夜班B组", "乙")

    def test_handoff_intent_is_persisted_not_just_returned(self, svc, datasets):
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        view = svc.begin_handoff(lease["租约"], "夜班B组", "接手人乙", "例行换班")
        # 重新从存储读取，确认接手执行者与原因已落库
        reloaded = svc.handoff_view(view["交接单"])
        assert reloaded["接手执行者"] == "接手人乙"
        assert reloaded["说明"] == "例行换班"
        assert reloaded["状态"] == "pending"


# ---------- 证据只追加 ----------
class TestEvidence:
    def test_evidence_is_content_addressed_and_reusable(self, svc):
        h1 = svc.add_evidence({"source": "nightly.json", "rows": 12})
        h2 = svc.add_evidence({"rows": 12, "source": "nightly.json"})  # 字段序不同
        assert h1 == h2
        record = svc.get_evidence(h1)
        assert record["payload_json"]  # 原始依据仍可复核

    def test_request_binds_evidence_and_write_needs_it(self, svc, datasets, store):
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets)
        h = svc.add_evidence({"source": "nightly.json"})
        req = svc.submit_request("orders@2026-11-17", "s2", None,
                                 idempotency_key="k-ev")
        # 手动补绑证据，模拟依据误删
        with store.connection() as db, db:
            db.execute("UPDATE replay_requests SET evidence_hash=? WHERE request_id=?",
                       (h, req["请求"]))
            db.execute("DELETE FROM evidence WHERE evidence_hash=?", (h,))
        svc.dispatch(req["请求"], lease["租约"], lease["fencing令牌"])
        with expect_code("EVIDENCE_NOT_FOUND"):
            svc.write(req["请求"], lease["租约"], lease["fencing令牌"])

    def test_evidence_table_has_no_update_or_delete_api(self, svc):
        methods = {name for name in dir(svc) if not name.startswith("_")}
        assert "delete_evidence" not in methods and "update_evidence" not in methods


# ---------- 过期 ----------
class TestExpiry:
    def test_expired_lease_guards_reject(self, tmp_path, datasets):
        state = {"calls": 0}

        def clock():
            state["calls"] += 1
            # 建租约与登记请求在首日；之后进入次日，租约已过期
            return ("2026-11-22T00:00:00+00:00" if state["calls"] <= 4
                    else "2026-11-23T00:00:00+00:00")

        store = LeaseStore(tmp_path / "l.sqlite", clock=clock)
        svc = LeaseService(store)
        lease = svc.acquire(["orders@2026-11-17"], "甲", "夜班A组", datasets,
                            ttl_seconds=3600)
        req = svc.submit_request("orders@2026-11-17", "s2", {"r": 1})
        # 时钟越过到期时间后，守卫拒绝并把租约置为 expired
        with expect_code("LEASE_EXPIRED"):
            svc.dispatch(req["请求"], lease["租约"], lease["fencing令牌"])
        assert svc.lease_view(lease["租约"])["状态"] == "expired"


# ---------- HTTP 端到端 ----------
class TestHttpApi:
    def test_full_handoff_cycle_over_http(self, http):
        cats = nightly_catalog(http)
        # 找 orders 分区
        payload = {"partitions": ["orders@2026-11-17"], "holder": "甲", "group": "夜班A组",
                   "sample": "nightly"}
        r = http.post("/api/lease/acquire", json=payload)
        assert r.status_code == 201
        lease = r.get_json()
        # 登记请求（带证据）
        r = http.post("/api/lease/requests", json={
            "partition": "orders@2026-11-17", "target_version": "s2",
            "evidence": {"source": "nightly.json", "why": "上游过期"},
            "idempotency_key": "http-1",
        })
        assert r.status_code == 201
        req = r.get_json()
        # 同键重试不新增
        r2 = http.post("/api/lease/requests", json={
            "partition": "orders@2026-11-17", "target_version": "s2",
            "evidence": {"source": "nightly.json", "why": "上游过期"},
            "idempotency_key": "http-1",
        })
        assert r2.status_code == 200 and r2.get_json()["请求"] == req["请求"]
        # 旧 token 写入先被拒（状态未派发）
        bad = http.post(f"/api/lease/requests/{req['请求']}/write",
                        json={"lease_id": lease["租约"], "token": lease["fencing令牌"]})
        assert bad.status_code == 422 and bad.get_json()["code"] == "REQUEST_STATE"
        # 交接
        ho = http.post("/api/lease/handoffs", json={
            "lease_id": lease["租约"], "to_group": "夜班B组", "to_holder": "乙",
        }).get_json()
        new_lease = http.post(f"/api/lease/handoffs/{ho['交接单']}/complete",
                              json={"to_holder": "乙"}).get_json()
        # 旧执行者落库被拒
        stale = http.post(f"/api/lease/requests/{req['请求']}/dispatch",
                          json={"lease_id": lease["租约"], "token": lease["fencing令牌"]})
        assert stale.status_code == 422 and stale.get_json()["code"] in (
            "LEASE_EXPIRED", "STALE_FENCING_TOKEN")
        # 新持有者完成派发→写入→回滚
        lid, tok = new_lease["租约"], new_lease["fencing令牌"]
        assert http.post(f"/api/lease/requests/{req['请求']}/dispatch",
                         json={"lease_id": lid, "token": tok}).status_code == 200
        assert http.post(f"/api/lease/requests/{req['请求']}/write",
                         json={"lease_id": lid, "token": tok}).status_code == 200
        assert http.post(f"/api/lease/requests/{req['请求']}/rollback",
                         json={"lease_id": lid, "token": tok}).status_code == 200
        # 仪表盘与只读页面
        board = http.get("/api/lease/dashboard").get_json()
        assert any(l["租约"] == lid for l in board["租约列表"])
        page = http.get("/console/lease")
        assert page.status_code == 200 and "交接租约".encode() in page.data
        # 证据可复核
        ev = http.get(f"/api/lease/evidence/{req['证据摘要']}")
        assert ev.status_code == 200

    def test_scope_conflict_over_http(self, http):
        http.post("/api/lease/acquire", json={
            "partitions": ["orders@2026-11-17"], "holder": "甲", "group": "A"})
        r = http.post("/api/lease/acquire", json={
            "partitions": ["facts@2026-11-17"], "holder": "乙", "group": "B"})
        assert r.status_code == 422 and r.get_json()["code"] == "SCOPE_CONFLICT"

    def test_dashboard_is_read_only_no_write_controls(self, http):
        page = http.get("/console/lease").data.decode()
        assert "<form" not in page
        assert "<button" not in page
