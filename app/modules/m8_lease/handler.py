from flask import Blueprint, current_app, jsonify, request

from app.keys import ReplayError
from app.modules.m2_graph.registry import catalog as build_catalog
from app.web import fixture

from .service import LeaseService

api = Blueprint("lease", __name__)


def lease_store():
    return current_app.extensions["leases"]


def service():
    return LeaseService(lease_store())


def datasets_from_payload(payload=None):
    """从清单样例或上传 JSON 中解析依赖注册表，作为租约分区校验依据。"""
    if payload is None or isinstance(payload, str):
        payload = fixture(payload or "nightly")
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    from app.modules.m1_inventory.reader import read_bundle

    bundle = read_bundle(payload)
    return build_catalog(bundle["datasets"])


def body():
    value = request.get_json(silent=True)
    if not isinstance(value, dict):
        raise ReplayError("JSON", "请求体须为 JSON 对象")
    return value


@api.get("/api/lease/dashboard")
def dashboard():
    return jsonify(service().dashboard(datasets_from_payload(None)))


@api.get("/api/lease/leases")
def list_leases():
    return jsonify([service().lease_view(row["lease_id"]) for row in lease_store().list_leases()])


@api.get("/api/lease/leases/<lease_id>")
def one_lease(lease_id):
    return jsonify(service().lease_view(lease_id))


@api.get("/api/lease/<key>")
def module_view(key):
    """符合模块视图契约：键集与 VIEWS['lease'] 的字段一致。

    key 为租约号时返回该租约；传入计划批次号等非租约标识时，回退为
    只读的当前租约概览，不做任何写入。
    """
    try:
        view = service().lease_view(key)
    except ReplayError as exc:
        if exc.code != "LEASE_NOT_FOUND":
            raise
        rows = lease_store().list_leases(limit=1)
        view = service().lease_view(rows[0]["lease_id"]) if rows else {"提示": "尚无租约"}
    return jsonify({"租约": view})


@api.post("/api/lease/acquire")
def acquire():
    args = body()
    datasets = datasets_from_payload(args.get("sample"))
    view = service().acquire(
        args.get("partitions"),
        args.get("holder", "值班工程师"),
        args.get("group", "未命名值班组"),
        datasets,
        note=args.get("note", ""),
    )
    return jsonify(view), 201


@api.post("/api/lease/handoffs")
def begin_handoff():
    args = body()
    view = service().begin_handoff(
        args["lease_id"], args["to_group"], args.get("to_holder", ""), args.get("reason", "")
    )
    return jsonify(view), 201


@api.post("/api/lease/handoffs/<handoff_id>/complete")
def complete_handoff(handoff_id):
    args = body()
    return jsonify(service().complete_handoff(handoff_id, args.get("to_holder", "接手工程师")))


@api.post("/api/lease/handoffs/<handoff_id>/reject")
def reject_handoff(handoff_id):
    args = body()
    return jsonify(service().reject_handoff(handoff_id, args.get("reason", "接手组拒绝")))


@api.post("/api/lease/leases/<lease_id>/release")
def release(lease_id):
    return jsonify(service().release_lease(lease_id))


@api.get("/api/lease/requests")
def list_requests():
    status = request.args.get("status")
    rows = lease_store().list_requests(status=status)
    return jsonify([service().request_view(row) for row in rows])


@api.post("/api/lease/requests")
def submit_request():
    args = body()
    view = service().submit_request(
        partition=args["partition"],
        target_version=args["target_version"],
        evidence_payload=args.get("evidence"),
        idempotency_key=args.get("idempotency_key"),
        detail=args.get("detail", ""),
    )
    return jsonify(view), 200 if view.get("reused") else 201


@api.post("/api/lease/requests/<request_id>/dispatch")
def dispatch(request_id):
    args = body()
    return jsonify(
        service().dispatch(request_id, args["lease_id"], args["token"])
    )


@api.post("/api/lease/requests/<request_id>/write")
def write(request_id):
    args = body()
    return jsonify(service().write(request_id, args["lease_id"], args["token"]))


@api.post("/api/lease/requests/<request_id>/rollback")
def rollback(request_id):
    args = body()
    return jsonify(service().rollback(request_id, args["lease_id"], args["token"]))


@api.get("/api/lease/evidence/<evidence_hash>")
def evidence(evidence_hash):
    return jsonify(service().get_evidence(evidence_hash))


@api.get("/api/lease/handoffs")
def handoffs():
    return jsonify([service().handoff_view(row["handoff_id"]) for row in lease_store().list_handoffs()])
