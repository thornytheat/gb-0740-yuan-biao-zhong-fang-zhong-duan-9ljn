# 深度登记 源表重放终端

- 规模：中型
- 核心规则：先扩展受影响的分区与跨日依赖，再生成写入计划，验证每个目标指针并演练原样回滚。

## 功能模块

| 模块 | 使用者动作 | 入口 | 核心文件 | 测试 |
|---|---|---|---|---|
| M1 分区登记 | 采集快照与变更并逐源行复核 | `GET /api/inventory/{id}` | `app/modules/m1_inventory/service.py` | `tests/test_inventory.py` |
| M2 依赖求解 | 查拓扑序、日期偏移与缺表 | `GET /api/graph/{id}` | `app/modules/m2_graph/service.py` | `tests/test_graph.py` |
| M3 清单指纹 | 核对预期产物版本与源数 | `GET /api/fingerprint/{id}` | `app/modules/m3_fingerprint/service.py` | `tests/test_fingerprint.py` |
| M4 重放排程 | 按状态检查挂起和依赖变化 | `GET /api/schedule/{id}` | `app/modules/m4_schedule/service.py` | `tests/test_schedule.py` |
| M5 回滚校验 | 核对写入前后与恢复哈希 | `GET /api/rollback/{id}` | `app/modules/m5_rollback/service.py` | `tests/test_rollback.py` |
| M6 行数勾稽 | 守恒比较影响分区、源记录和行数 | `GET /api/reconcile/{id}` | `app/modules/m6_reconcile/service.py` | `tests/test_reconcile.py` |
| M7 版本签发 | 在近期计划中复用并下载清单 | `GET /api/release/{id}` | `app/modules/m7_release/service.py` | `tests/test_release.py` |
| M8 交接租约 | 按依赖分量持有、fencing 守卫派发/写入/回滚与两阶段交接 | `GET /api/lease/dashboard` | `app/modules/m8_lease/service.py` | `tests/test_lease.py` |

## 陷阱登记

| TRAP | 现象 | 朴素实现会怎样 | 本项目处理 | 处理位置 | 测试 |
|---|---|---|---|---|---|
| TRAP-02 | clean.json 加 BOM | 首字段异常 | UTF-8 BOM 解码 | `app/modules/m1_inventory/reader.py` | `tests/test_inventory.py::test_bom` |
| TRAP-06 | nightly.json 的行数全角千分位 | 按原文解析失败 | 正则后归一 | `app/keys.py` | `tests/test_inventory.py::test_fullwidth_grouping` |
| TRAP-07 | nightly.json 行数 N/A | 当作零 | 隔离无效变更 | `app/modules/m1_inventory/service.py` | `tests/test_inventory.py::test_missing_value_isolated` |
| TRAP-09 | 超大汇总整数由两个大分区产生 | 浏览器舍入 | 安全范围外用字符串 | `app/modules/m6_reconcile/service.py` | `tests/test_reconcile.py::test_aggregated_large_counts_are_exact_strings` |
| TRAP-14 | nightly.json 同分区改两次 | 最后一次覆盖 | 整组隔离 | `app/modules/m1_inventory/service.py` | `tests/test_inventory.py::test_conflict_both_reviewed` |
| TRAP-16 | clean.json 尾部截断 | 保留半表 | 结构完整才计算 | `app/modules/m1_inventory/reader.py` | `tests/test_inventory.py::test_truncated_input_rejected` |
| TRAP-17 | nightly.json 旧字段 count | 字段缺失 | 择一读取旧别名 | `app/modules/m1_inventory/service.py` | `tests/test_inventory.py::test_legacy_count` |
| TRAP-20 | clean.json 添加回边 | 排序丢结点或死循环 | 深度优先检测访问栈 | `app/modules/m2_graph/service.py` | `tests/test_graph.py::test_cycle_path_from_sample` |
| TRAP-23 | nightly.json 截点紧邻滞后一日 | 越界写入 | 分区日显式切界 | `app/modules/m4_schedule/expansion.py` | `tests/test_schedule.py::test_cutoff_included_but_lagged_next_day_excluded` |
| TRAP-26 | 演练前计划引用旧指针 | 覆盖新版本 | 检查所有写入前置值 | `app/modules/m5_rollback/service.py` | `tests/test_rollback.py::test_stale_snapshot_atomic` |
| TRAP-30 | 两个值班组同时重放同一依赖链 | 相互依赖的分区拆给不同持有者，旧执行者继续写入 | 以无向连通分量为持有边界，闭包内冲突整组拒绝，互不相连分量可并行 | `app/modules/m8_lease/graph.py` | `tests/test_lease.py::TestAcquire` |
| TRAP-31 | 交接后旧执行者仍持旧令牌提交 | 旧令牌写入成功、数据错乱 | 库内单调 fencing token，交接即作废旧租约，守卫拒绝旧令牌落库 | `app/modules/m8_lease/service.py` | `tests/test_lease.py::TestFencing::test_old_executor_blocked_after_handoff` |
| TRAP-32 | 同一请求重试产生第二份 | 重复派发、重复落库 | 幂等键唯一约束，重试复用先到请求，不新增 | `app/modules/m8_lease/service.py` | `tests/test_lease.py::TestRequests::test_retry_same_idempotency_key_does_not_duplicate` |
| TRAP-33 | 交接失败后旧范围失联 | 单据被删、无法追踪 | 交接单只标记 rejected/expired 不删除，旧租约保持活动并保留范围 | `app/modules/m8_lease/service.py` | `tests/test_lease.py::TestHandoff::test_rejected_handoff_keeps_old_lease_and_audit_trail` |
| TRAP-34 | 放行的原始依据被误删 | 无法复核当时为何放行 | 证据按内容哈希只追加存储，写入前必须能取到证据，缺失即拒绝 | `app/modules/m8_lease/service.py` | `tests/test_lease.py::TestEvidence` |
| TRAP-35 | 到期状态随拒绝事务回滚 | 租约表面仍活动、可再用 | 写事务前用独立连接先落库到期标记，再在主事务守卫拒绝 | `app/modules/m8_lease/service.py` | `tests/test_lease.py::TestExpiry` |

## 已知边界

清单约定按日分区、拼接型节点、离线镜像演练。读源快照与分区日的证据随签发清单输出。
