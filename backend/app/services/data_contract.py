"""数据契约领域服务：数据消费者与生产者之间的契约管理、Schema约束与SLA跟踪。

改造要点：
- 契约不再直接从语义模型行反推口径，而是从「投产映射确认批次」消费已经冻结的真实
  模型映射，生成可校验的约束版本（``contract_constraint_version``），并把状态结果
  回写到对应发布清单（``result_mart``）；
- 契约字段变更写入字段变更记录（``contract_field_change``），不再原地改写语义模型
  的字段别名，避免两套口径争用同一条投产记录；受影响模型标记为待重新确认。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.promotion_confirm_batch import (
    CONFIRM_LOCK,
    build_contract_snapshot,
    fingerprint_of,
)
from app.store import store


class DataContractService(BaseModuleService):
    MODULE = "data_contract"
    ENTITY = "契约定义"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["契约编号", "数据集", "Schema约束", "SLA定义", "更新频率", "负责人", "消费者", "生效时间"]
    REQUIRED_FIELDS = ["契约编号", "数据集"]
    STATUS_ORDER = ["草稿", "已承诺", "已违约", "已终止"]
    ACTION_RULES = {"签署契约": "已承诺", "标记违约": "已违约", "终止契约": "已终止"}
    NEGATIVE_ACTIONS = ["标记违约", "终止契约"]

    # ------------------------------------------------------------------
    # 从投产映射确认批次消费真实模型映射
    # ------------------------------------------------------------------
    def consume_confirmation_batch(self, batch_id: int) -> dict[str, Any]:
        """消费确认批次：生成可校验约束版本，并把状态结果回写发布清单。

        全程在 ``CONFIRM_LOCK`` 内完成，与投产确认共享同一把锁，避免两端并发操作
        产生半更新。同一批次重复消费只返回首次生成的约束版本。
        """
        with CONFIRM_LOCK:
            batch = store.find("promotion_confirm_batch", batch_id)
            if batch is None:
                return {"ok": False, "message": f"确认批次 {batch_id} 不存在"}
            if batch["status"] not in ("确认通过", "已消费"):
                return {
                    "ok": False,
                    "message": f"批次状态为{batch['status']}，只有确认通过的批次可被契约消费",
                }

            # 幂等：同一确认批只生成一个约束版本、一份发布清单回写。
            existing = next(
                (row for row in store.rows("contract_constraint_version")
                 if int(row.get("批次ID", 0)) == batch_id),
                None,
            )
            if existing is not None:
                return {
                    "ok": True, "reused": True,
                    "batch_id": batch_id,
                    "约束版本": existing["约束版本"],
                    "约束版本ID": existing["id"],
                    "发布清单ID": existing.get("发布清单ID"),
                    "message": "该批次已消费，返回既有约束版本",
                }

            contract = store.find("data_contract", int(batch["关联契约"]))
            if contract is None:
                return {"ok": False, "message": "批次关联的数据契约已不存在"}

            # 漂移护栏：契约在批次确认后若被换成另一套列定义，拒绝消费并要求重新确认。
            live_contract_snapshot = build_contract_snapshot(contract)
            live_contract_fp = fingerprint_of(live_contract_snapshot)
            if live_contract_fp != batch.get("契约指纹"):
                return {
                    "ok": False,
                    "message": "契约列定义已偏离确认批次快照（口径争用），请重新提交确认批后再消费",
                    "batch_contract_fp": batch.get("契约指纹"),
                    "live_contract_fp": live_contract_fp,
                }

            model_snapshot = batch["模型快照"]
            # 约束版本直接来自批次冻结的真实模型映射，而不是再次读取模型行。
            constraint_columns = list(dict.fromkeys(
                model_snapshot["源字段"]
                + model_snapshot["聚合定义"]
                + model_snapshot["维度定义"]
            ))
            constraint_payload = {
                "contract_id": contract["id"],
                "批次编号": batch["批次编号"],
                "模型编号": model_snapshot["模型编号"],
                "模型版本": model_snapshot["版本序号"],
                "模型指纹": batch["模型指纹"],
                "契约指纹": batch["契约指纹"],
                "columns": constraint_columns,
                "计算口径": model_snapshot["计算口径"],
                "快照": {"模型": model_snapshot, "契约": live_contract_snapshot},
            }
            version_no = self._next_version_no(contract["id"])
            constraint_hash = fingerprint_of(constraint_payload)

            version_rows = store.rows("contract_constraint_version")
            version_row = {
                "id": max((int(row.get("id", 0)) for row in version_rows), default=0) + 1,
                "约束版本": version_no,
                "约束编号": f"CV-{uuid.uuid4().hex[:8].upper()}",
                "契约编号": contract.get("契约编号", ""),
                "契约ID": contract["id"],
                "批次ID": batch_id,
                "批次编号": batch["批次编号"],
                "模型编号": model_snapshot["模型编号"],
                "模型版本": model_snapshot["版本序号"],
                "模型指纹": batch["模型指纹"],
                "约束字段": ",".join(constraint_columns),
                "约束哈希": constraint_hash,
                "status": "生效中",
                "生成时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "pending": False,
                "abnormal": False,
            }
            version_rows.append(version_row)

            # 旧的生效版本归档，保证同一时刻只有一个可校验的当前约束版本。
            for row in version_rows:
                if int(row.get("契约ID", 0)) == int(contract["id"]) and row["id"] != version_row["id"]:
                    row["status"] = "已归档"
                    row["pending"] = False

            manifest_id = self._write_release_manifest(contract, batch, version_no, constraint_hash)
            version_row["发布清单ID"] = manifest_id

            # 契约状态结果回写：当前约束版本可校验、与批次指纹绑定。
            contract["当前约束版本"] = version_no
            contract["约束哈希"] = constraint_hash
            contract["确认批次编号"] = batch["批次编号"]
            contract["映射指纹"] = batch["模型指纹"]
            contract["模型版本"] = model_snapshot["版本序号"]
            contract["Schema约束"] = ",".join(constraint_columns)
            contract["status"] = "已承诺"
            contract["pending"] = False
            contract["abnormal"] = False

            batch["status"] = "已消费"
            batch["确认结果"] = "已消费"
            batch["pending"] = False
            batch["约束版本"] = version_no
            self._audit(f"CONTRACT_CONSUME batch={batch['批次编号']} version={version_no}")

            return {
                "ok": True, "reused": False,
                "batch_id": batch_id,
                "约束版本": version_no,
                "约束版本ID": version_row["id"],
                "约束字段": constraint_columns,
                "约束哈希": constraint_hash,
                "发布清单ID": manifest_id,
            }

    def verify_contract_compliance(self, contract_id: int) -> dict[str, Any]:
        """验证数据契约的合规性，跨模块检查Schema一致性和数据新鲜度。

        跨模块数据流：
        - 读取 source_registry 校验数据源状态
        - 读取 contract_constraint_version / 确认批指纹校验Schema一致性
        - 读取 freshness_mon 检查SLA达标情况
        - 读取 query_audit 检查消费记录
        """
        contract = store.find(self.MODULE, contract_id)
        if contract is None:
            return {"ok": False, "message": "数据契约不存在"}

        dataset = contract.get("数据集", "")
        violations: list[str] = []

        # 检查数据源是否存在且可用
        sources = store.rows("source_registry")
        source_ok = False
        for s in sources:
            if s.get("源编号", "") in dataset or dataset in str(s.get("连接地址", "")):
                if s.get("status") == "已停用":
                    violations.append(f"数据源{s['源编号']}已停用")
                else:
                    source_ok = True

        if not source_ok:
            violations.append("未找到对应的可用数据源")

        # 检查Schema一致性：优先用约束版本指纹与确认批快照核对
        models = store.rows("semantic_model")
        schema_ok = False
        active_version = next(
            (row for row in store.rows("contract_constraint_version")
             if int(row.get("契约ID", 0)) == contract_id and row.get("status") == "生效中"),
            None,
        )
        bound_batch_no = contract.get("确认批次编号")
        bound_batch = next(
            (row for row in store.rows("promotion_confirm_batch")
             if row.get("批次编号") == bound_batch_no),
            None,
        )
        model = next(
            (m for m in models
             if m.get("status") == "已投产" and dataset in str(m.get("源表映射", ""))),
            None,
        )
        if active_version is not None and bound_batch is not None and model is not None:
            if active_version.get("模型指纹") == bound_batch.get("模型指纹") \
                    and contract.get("映射指纹") == bound_batch.get("模型指纹"):
                if model.get("投产指纹") == bound_batch.get("模型指纹"):
                    schema_ok = True
                else:
                    violations.append("当前模型映射已偏离约束版本绑定的确认快照")
            else:
                violations.append("约束版本与确认批次的模型指纹不一致")
        elif model is not None:
            # 兼容尚未走确认批的历史模型版本：保留旧的字符串比较口径。
            if model.get("字段别名", "") == contract.get("Schema约束", ""):
                schema_ok = True
            else:
                violations.append("Schema定义与语义模型不一致")
        else:
            violations.append("缺少已投产语义模型或可校验约束版本")

        # 检查SLA新鲜度
        freshness_rows = store.rows("freshness_mon")
        freshness_ok = True
        for f in freshness_rows:
            if dataset in str(f.get("目标数据集", "")):
                if f.get("status") == "已告警":
                    freshness_ok = False
                    violations.append(f"新鲜度告警: {f.get('延迟阈值', 'unknown')}")

        # 检查消费记录
        audits = store.rows("query_audit")
        consumer = contract.get("消费者", "")
        consumption_records = [a for a in audits if consumer in str(a.get("操作用户", ""))]
        if not consumption_records:
            violations.append("消费者无实际查询记录")

        # 更新契约状态
        if violations:
            contract["status"] = "已违约"
            contract["pending"] = True
            contract["abnormal"] = True
        else:
            contract["status"] = "已承诺"
            contract["pending"] = False
            contract["abnormal"] = False

        return {
            "ok": True,
            "contract_id": contract_id,
            "status": contract["status"],
            "violations": violations,
            "has_source": source_ok,
            "has_schema_match": schema_ok,
            "is_fresh": freshness_ok,
            "consumption_records": len(consumption_records),
            "active_constraint_version": contract.get("当前约束版本"),
        }

    def propagate_schema_change(self, contract_id: int, new_schema: str) -> dict[str, Any]:
        """当Schema变化时记录契约字段变更并通知消费者重新确认。

        跨模块数据流：
        - 写入 contract_field_change 字段变更记录（保留旧列定义，可审计）
        - 更新 data_contract 的Schema约束并升级版本标记
        - 标记受影响 semantic_model 待重新确认，但不原地改写模型字段，避免口径争用
        - 写入 query_audit
        """
        contract = store.find(self.MODULE, contract_id)
        if contract is None:
            return {"ok": False, "message": "数据契约不存在"}

        old_schema = contract.get("Schema约束", "")
        old_version = contract.get("当前约束版本", "")

        # 字段变更记录独立留痕，确认批快照仍保留变更前的完整列定义。
        change_rows = store.rows("contract_field_change")
        change_row = {
            "id": max((int(row.get("id", 0)) for row in change_rows), default=0) + 1,
            "变更编号": f"CFC-{uuid.uuid4().hex[:8].upper()}",
            "契约编号": contract.get("契约编号", ""),
            "契约ID": contract_id,
            "旧Schema": old_schema,
            "新Schema": new_schema,
            "旧约束版本": old_version,
            "变更时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "待确认",
            "pending": True,
            "abnormal": False,
        }
        change_rows.append(change_row)

        contract["Schema约束"] = new_schema
        contract["pending"] = True
        # 列定义已切换：挂起旧约束版本的绑定，等待新确认批消费后再生成新版本。
        contract["待确认字段变更"] = change_row["变更编号"]
        contract["当前约束版本"] = None

        # 通知语义模型重新确认，不直接覆盖模型字段（防止两套口径争用同一投产记录）。
        # 匹配范围：确认批绑定过该契约的模型，或源表映射/名称与数据集相关的模型。
        bound_model_ids = {
            int(row.get("关联模型"))
            for row in store.rows("promotion_confirm_batch")
            if int(row.get("关联契约", 0)) == contract_id and row.get("关联模型")
        }
        affected_models: list[str] = []
        models = store.rows("semantic_model")
        for m in models:
            related = (
                int(m["id"]) in bound_model_ids
                or contract.get("数据集", "") in str(m.get("源表映射", ""))
                or str(m.get("模型编号", "")) in str(contract.get("数据集", ""))
            )
            if related:
                m["pending"] = True
                m["待确认契约变更"] = change_row["变更编号"]
                affected_models.append(str(m.get("模型编号", m["id"])))

        self._audit(
            f"CONTRACT_SCHEMA_CHANGE contract={contract_id} change={change_row['变更编号']}"
        )

        return {
            "ok": True,
            "contract_id": contract_id,
            "old_schema": old_schema,
            "new_schema": new_schema,
            "change_id": change_row["变更编号"],
            "affected_models": affected_models,
        }

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    def _next_version_no(self, contract_id: int) -> str:
        existing = [
            row for row in store.rows("contract_constraint_version")
            if int(row.get("契约ID", 0)) == contract_id
        ]
        return f"cv{len(existing) + 1}.0"

    def _write_release_manifest(
        self, contract: dict[str, Any], batch: dict[str, Any],
        version_no: str, constraint_hash: str,
    ) -> int:
        """把约束版本的状态结果回写到对应发布清单（result_mart）。"""
        rows = store.rows("result_mart")
        manifest = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "集市编号": f"RLS-{uuid.uuid4().hex[:8].upper()}",
            "集市名称": f"发布清单-{contract.get('契约编号', contract['id'])}",
            "数据来源": f"promotion_confirm_batch:{batch['id']}",
            "主题域": contract.get("数据集", ""),
            "行数统计": str(len(batch.get("模型快照", {}).get("源字段", []))),
            "刷新周期": contract.get("更新频率", "manual"),
            "下游消费": contract.get("消费者", ""),
            "最后刷新": time.strftime("%Y-%m-%d %H:%M:%S"),
            "确认批次编号": batch["批次编号"],
            "约束版本": version_no,
            "约束哈希": constraint_hash,
            "状态结果": "已发布",
            "status": "可用",
            "pending": False,
            "abnormal": False,
        }
        rows.append(manifest)
        return manifest["id"]

    def _audit(self, statement: str) -> None:
        rows = store.rows("query_audit")
        rows.append({
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
            "查询语句": statement,
            "执行耗时": "120ms",
            "扫描行数": "1",
            "返回行数": "1",
            "数据来源": "data_contract",
            "操作用户": "contract_manager",
            "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "正常",
            "pending": False,
            "abnormal": False,
        })
