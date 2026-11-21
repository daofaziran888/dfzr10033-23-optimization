"""投产映射确认批次领域服务。

背景：语义模型投产与数据契约更新原先各自读取源表映射 / 契约字段变更记录再分别
回写，重复取数且容易产生口径不一致。本服务把一次投产校验收敛为一个**确认批次**：

- 批次创建时一次性读取语义模型的完整映射快照（源字段、字段别名、聚合定义）与数据
  契约的完整列定义快照，后续校验 / 消费 / 回写全部只读快照，不再重复取数；
- 幂等键由「模型 + 模型指纹 + 契约 + 契约指纹」构成，同一校验批重复提交只保留一个
  确认结果；失败批次可以重新进入续跑，任何失败都不会产生半更新的定义；
- 批次持久化双方完整快照与指纹，历史批次可还原当时确认的映射 × 契约组合。

并发约定：``CONFIRM_LOCK`` 是确认 / 消费 / 重入流程共享的进程内锁，契约侧消费批次
时复用同一把锁，保证「校验 + 投产 + 约束版本写入 + 发布清单回写」的原子性。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 投产确认链路共享锁：semantic_model 提交确认、data_contract 消费批次都走这把锁。
CONFIRM_LOCK = threading.RLock()

_SOURCE_REF_PATTERN = re.compile(r"source_registry:(\d+)")


def canonical_json(payload: Any) -> str:
    """稳定序列化：同样的内容永远得到同样的字符串，指纹才可跨重放比较。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint_of(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()[:16]


def split_terms(raw: Any) -> list[str]:
    return [term.strip() for term in str(raw or "").split(",") if term.strip()]


def model_version_of(model: dict[str, Any]) -> str:
    # 历史模型版本可能没有版本序号，给一个稳定兜底，保证老数据也能参与指纹与幂等。
    return str(model.get("版本序号") or f"unversioned-m{model.get('id', 0)}")


def contract_version_of(contract: dict[str, Any]) -> str:
    return str(contract.get("约束版本") or f"v0-{contract.get('生效时间', 'na')}")


def build_model_snapshot(model: dict[str, Any]) -> dict[str, Any]:
    """一次性快照模型投产所需的完整映射：源字段、别名、聚合定义同时可读。"""
    source_ids = [int(part) for part in _SOURCE_REF_PATTERN.findall(str(model.get("源表映射", "")))]
    sources: list[dict[str, Any]] = []
    for source in store.rows("source_registry"):
        if int(source.get("id", 0)) in source_ids:
            sources.append({
                "id": int(source["id"]),
                "源编号": source.get("源编号", ""),
                "源类型": source.get("源类型", ""),
                "接入状态": source.get("接入状态", ""),
            })
    # 注意：快照只冻结业务定义，不纳入 status / 投产指纹等运行态字段，
    # 否则确认回写会改变指纹，破坏同一校验批的幂等键稳定性。
    return {
        "kind": "semantic_model",
        "id": int(model["id"]),
        "模型编号": model.get("模型编号", ""),
        "模型名称": model.get("模型名称", ""),
        "版本序号": model_version_of(model),
        "源表映射": model.get("源表映射", ""),
        "源引用": source_ids,
        "源登记": sources,
        # 待推广的源字段与别名
        "源字段": split_terms(model.get("字段别名")),
        "字段别名": split_terms(model.get("字段别名")),
        # 聚合 / 度量定义
        "聚合定义": split_terms(model.get("度量定义")),
        "度量定义": split_terms(model.get("度量定义")),
        "维度定义": split_terms(model.get("维度定义")),
        "计算口径": model.get("计算口径", ""),
    }


def build_contract_snapshot(contract: dict[str, Any]) -> dict[str, Any]:
    """一次性快照契约的完整列定义与当前生效约束。

    仅冻结列定义等业务内容；status、当前约束版本、绑定指纹等运行态字段不参与指纹，
    保证确认回写后同一口径仍命中同一幂等键。
    """
    return {
        "kind": "data_contract",
        "id": int(contract["id"]),
        "契约编号": contract.get("契约编号", ""),
        "数据集": contract.get("数据集", ""),
        "版本": contract_version_of(contract),
        "Schema约束": contract.get("Schema约束", ""),
        "契约列": split_terms(contract.get("Schema约束")),
        "SLA定义": contract.get("SLA定义", ""),
        "更新频率": contract.get("更新频率", ""),
        "消费者": contract.get("消费者", ""),
        "生效时间": contract.get("生效时间", ""),
    }


def idempotency_key(model_id: int, model_fp: str, contract_id: int, contract_fp: str) -> str:
    return f"PC::m{model_id}:{model_fp}::c{contract_id}:{contract_fp}"


class PromotionConfirmBatchService(BaseModuleService):
    MODULE = "promotion_confirm_batch"
    ENTITY = "投产映射确认批次"
    OPERATION_TERM = "确认"
    LIST_FIELDS = [
        "批次编号", "模型编号", "契约编号", "模型版本", "契约版本", "幂等键",
        "模型指纹", "契约指纹", "确认结果", "确认时间", "失败原因", "重入次数", "来源批次",
    ]
    REQUIRED_FIELDS = ["批次编号", "模型编号", "契约编号", "幂等键", "模型指纹", "契约指纹"]
    STATUS_ORDER = ["待确认", "确认通过", "确认失败", "已消费"]
    ACTION_RULES = {
        "重新确认": "待确认",
        "标记通过": "确认通过",
        "标记失败": "确认失败",
        "标记消费": "已消费",
    }
    NEGATIVE_ACTIONS = ["标记失败"]

    # ------------------------------------------------------------------
    # 核心流程：提交确认批（幂等 + 可重入 + 原子提交）
    # ------------------------------------------------------------------
    def submit_confirmation(self, model_id: int, contract_id: int) -> dict[str, Any]:
        with CONFIRM_LOCK:
            model = store.find("semantic_model", model_id)
            contract = store.find("data_contract", contract_id)
            if model is None:
                return {"ok": False, "message": f"语义模型 {model_id} 不存在，无法建立确认批"}
            if contract is None:
                return {"ok": False, "message": f"数据契约 {contract_id} 不存在，无法建立确认批"}

            # 一次性读取两份完整快照，之后所有阶段都只用快照，避免重复取数与中途漂移。
            model_snapshot = build_model_snapshot(model)
            contract_snapshot = build_contract_snapshot(contract)
            model_fp = fingerprint_of(model_snapshot)
            contract_fp = fingerprint_of(contract_snapshot)
            idem_key = idempotency_key(model_id, model_fp, contract_id, contract_fp)

            # 1) 同一幂等键已有成功/已消费批次：重复提交直接回放唯一确认结果。
            idem_batch = self._find_by_idem_key(idem_key)
            if idem_batch is not None and idem_batch["status"] in ("确认通过", "已消费"):
                return self._result(idem_batch, reused=True)

            # 2) 同一模型×契约存在失败批次：无论定义是否已修复，都重新进入同批续跑，
            #    不新增确认结果（失败重入是同一条业务动作的延续）。
            failed_batch = self._latest_failed_batch(model_id, contract_id)
            reentered = False
            batch: dict[str, Any] | None = failed_batch
            if batch is not None:
                reentered = True
                batch["重入次数"] = int(batch.get("重入次数", 0)) + 1
                batch["status"] = "待确认"
                batch["pending"] = True
                batch["失败原因"] = ""
                # 重入时以最新定义刷新快照与幂等键，批次记录仍是同一条。
                batch["幂等键"] = idem_key
                batch["模型指纹"] = model_fp
                batch["契约指纹"] = contract_fp
                batch["模型版本"] = model_snapshot["版本序号"]
                batch["契约版本"] = contract_snapshot["版本"]
                batch["模型快照"] = model_snapshot
                batch["契约快照"] = contract_snapshot
            else:
                # 3) 全新口径（定义或契约列确实变了）才产生新批次，历史批次原样保留。
                batch = self._create_batch(
                    model, contract, model_snapshot, contract_snapshot,
                    model_fp, contract_fp, idem_key,
                )

            issues = self._validate_snapshots(model_snapshot, contract_snapshot)
            if issues:
                # 失败只落批次状态，绝不改模型 / 契约定义，杜绝半更新。
                batch["status"] = "确认失败"
                batch["确认结果"] = "失败"
                batch["失败原因"] = "; ".join(issues)
                batch["确认时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
                batch["pending"] = True
                batch["abnormal"] = True
                self._audit(batch, "CONFIRM_FAILED", detail=batch["失败原因"])
                return self._result(batch, reused=False, reentered=reentered, issues=issues)

            # 校验通过：在同一把锁内原子提交模型投产与契约绑定。
            model["status"] = "已投产"
            model["pending"] = False
            model["abnormal"] = False
            model["投产确认批次"] = batch["批次编号"]
            model["投产指纹"] = model_fp

            contract["确认批次编号"] = batch["批次编号"]
            contract["映射指纹"] = model_fp
            contract["模型版本"] = model_snapshot["版本序号"]
            contract["pending"] = True

            batch["status"] = "确认通过"
            batch["确认结果"] = "通过"
            batch["失败原因"] = ""
            batch["确认时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
            batch["pending"] = False
            batch["abnormal"] = False
            batch["模型快照"] = model_snapshot
            batch["契约快照"] = contract_snapshot
            self._audit(batch, "CONFIRM_PASSED")
            return self._result(batch, reused=False, reentered=reentered)

    def reenter_confirmation(self, batch_id: int) -> dict[str, Any]:
        """失败批次重新进入：按批次记录的模型 / 契约重新提交，走同一套幂等逻辑。"""
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"ok": False, "message": f"确认批次 {batch_id} 不存在"}
        if batch["status"] != "确认失败":
            return {"ok": False, "message": f"批次状态为{batch['status']}，仅确认失败的批次可重新进入"}
        return self.submit_confirmation(int(batch["关联模型"]), int(batch["关联契约"]))

    # ------------------------------------------------------------------
    # 查询 / 还原 / 对账
    # ------------------------------------------------------------------
    def get_batch_detail(self, batch_id: int) -> dict[str, Any]:
        """返回批次完整内容，可据此还原当时确认的映射 × 契约组合。"""
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"ok": False, "message": f"确认批次 {batch_id} 不存在"}
        versions = [
            row for row in store.rows("contract_constraint_version")
            if int(row.get("批次ID", 0)) == batch_id
        ]
        return {"ok": True, "batch": batch, "constraint_versions": versions}

    def latest_batch_for_pair(self, model_id: int, contract_id: int) -> dict[str, Any] | None:
        rows = [
            row for row in store.rows(self.MODULE)
            if int(row.get("关联模型", 0)) == model_id and int(row.get("关联契约", 0)) == contract_id
        ]
        return max(rows, key=lambda row: int(row["id"]), default=None)

    def refresh_and_reconcile(self, model_id: int, contract_id: int) -> dict[str, Any]:
        """并发操作后刷新模型与契约，核对版本、字段与使用中的数据口径是否对得上。"""
        with CONFIRM_LOCK:
            model = store.find("semantic_model", model_id)
            contract = store.find("data_contract", contract_id)
            if model is None or contract is None:
                return {"ok": False, "message": "模型或契约不存在，无法刷新对账"}
            live_model_snapshot = build_model_snapshot(model)
            live_contract_snapshot = build_contract_snapshot(contract)
            live_model_fp = fingerprint_of(live_model_snapshot)
            live_contract_fp = fingerprint_of(live_contract_snapshot)

            batch = self.latest_batch_for_pair(model_id, contract_id)
            mismatches: list[str] = []
            if batch is None:
                mismatches.append("模型与契约之间尚无确认批次")
            else:
                if batch.get("模型指纹") != live_model_fp:
                    mismatches.append("模型字段/口径已偏离确认批次快照，需重新提交确认")
                active_version = contract.get("当前约束版本")
                if batch["status"] == "已消费":
                    # 消费后契约会以约束字段重写列定义，契约快照指纹不再适合作为漂移判据；
                    # 此时以「当前约束版本 ↔ 批次模型指纹」的绑定关系为准。
                    if batch.get("契约指纹") != live_contract_fp and not active_version:
                        mismatches.append("契约列定义已切换，与确认批次快照不一致")
                    if not active_version:
                        mismatches.append("批次已消费但契约缺少当前约束版本，需重新确认并消费")
                    else:
                        version_row = next(
                            (row for row in store.rows("contract_constraint_version")
                             if row.get("约束版本") == active_version),
                            None,
                        )
                        if version_row is None:
                            mismatches.append(f"契约当前约束版本 {active_version} 找不到可校验记录")
                        elif version_row.get("模型指纹") != live_model_fp:
                            mismatches.append("使用中的约束版本与当前模型映射指纹不一致")
                        if contract.get("映射指纹") != batch.get("模型指纹"):
                            mismatches.append("契约绑定的映射指纹与确认批次不一致")
                else:
                    # 尚未消费：契约列定义必须仍与确认时的快照一致，防止两套口径争用。
                    if batch.get("契约指纹") != live_contract_fp:
                        mismatches.append("契约列定义已切换，与确认批次快照不一致")
                    if active_version:
                        mismatches.append("契约在批次消费前已存在约束版本，状态异常")

            manifest = next(
                (row for row in store.rows("result_mart")
                 if row.get("主题域") == contract.get("数据集")
                 and f"promotion_confirm_batch" in str(row.get("数据来源", ""))),
                None,
            )
            return {
                "ok": True,
                "aligned": not mismatches,
                "mismatches": mismatches,
                "model": {
                    "id": model_id, "版本序号": model_version_of(model),
                    "status": model.get("status"), "fingerprint": live_model_fp,
                },
                "contract": {
                    "id": contract_id, "版本": contract_version_of(contract),
                    "status": contract.get("status"), "fingerprint": live_contract_fp,
                    "当前约束版本": contract.get("当前约束版本"),
                },
                "active_batch": None if batch is None else {
                    "id": batch["id"], "批次编号": batch["批次编号"], "status": batch["status"],
                    "模型指纹": batch.get("模型指纹"), "契约指纹": batch.get("契约指纹"),
                },
                "release_manifest": None if manifest is None else {
                    "id": manifest["id"], "集市编号": manifest.get("集市编号"),
                    "status": manifest.get("status"),
                },
                "restorable": batch is not None and "模型快照" in batch and "契约快照" in batch,
            }

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    def _find_by_idem_key(self, idem_key: str) -> dict[str, Any] | None:
        return next(
            (row for row in store.rows(self.MODULE) if row.get("幂等键") == idem_key),
            None,
        )

    def _latest_failed_batch(self, model_id: int, contract_id: int) -> dict[str, Any] | None:
        rows = [
            row for row in reversed(store.rows(self.MODULE))
            if int(row.get("关联模型", 0)) == model_id
            and int(row.get("关联契约", 0)) == contract_id
            and row.get("status") == "确认失败"
        ]
        return rows[0] if rows else None

    def _create_batch(
        self, model: dict[str, Any], contract: dict[str, Any],
        model_snapshot: dict[str, Any], contract_snapshot: dict[str, Any],
        model_fp: str, contract_fp: str, idem_key: str,
    ) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        previous = next(
            (row for row in reversed(rows)
             if int(row.get("关联模型", 0)) == int(model["id"])
             and int(row.get("关联契约", 0)) == int(contract["id"])
             and row.get("status") == "确认失败"),
            None,
        )
        batch = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "批次编号": f"PCB-{uuid.uuid4().hex[:8].upper()}",
            "关联模型": int(model["id"]),
            "关联契约": int(contract["id"]),
            "模型编号": model.get("模型编号", ""),
            "契约编号": contract.get("契约编号", ""),
            "模型版本": model_snapshot["版本序号"],
            "契约版本": contract_snapshot["版本"],
            "幂等键": idem_key,
            "模型指纹": model_fp,
            "契约指纹": contract_fp,
            "确认结果": "待确认",
            "确认时间": "",
            "失败原因": "",
            "重入次数": 0,
            "来源批次": previous["批次编号"] if previous else "",
            # 先挂快照，即使失败也保留读取时的完整口径，便于排查与还原。
            "模型快照": model_snapshot,
            "契约快照": contract_snapshot,
            "status": "待确认",
            "pending": True,
            "abnormal": False,
        }
        rows.append(batch)
        return batch

    def _validate_snapshots(
        self, model_snapshot: dict[str, Any], contract_snapshot: dict[str, Any],
    ) -> list[str]:
        issues: list[str] = []
        if not model_snapshot["源字段"]:
            issues.append("字段别名/源字段不能为空")
        if not model_snapshot["聚合定义"]:
            issues.append("聚合（度量）定义不能为空")
        # 运行态（停用/终止）以实时行判断，不纳入定义快照指纹。
        live_sources = {int(row["id"]): row for row in store.rows("source_registry")}
        if model_snapshot["源引用"] and not any(
            sid in live_sources for sid in model_snapshot["源引用"]
        ):
            issues.append("关联的数据源不存在或已停用")
        for sid in model_snapshot["源引用"]:
            source = live_sources.get(sid)
            if source is not None and (
                source.get("status") == "已停用" or source.get("接入状态") == "已停用"
            ):
                issues.append(f"数据源{source.get('源编号', sid)}已停用")
        live_contract = store.find("data_contract", contract_snapshot["id"])
        if live_contract is not None and live_contract.get("status") == "已终止":
            issues.append("数据契约已终止，不能再确认投产")
        return issues

    def _audit(self, batch: dict[str, Any], action: str, detail: str = "") -> None:
        rows = store.rows("query_audit")
        rows.append({
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
            "查询语句": f"{action} batch={batch['批次编号']}",
            "执行耗时": "80ms",
            "扫描行数": "2",
            "返回行数": "1",
            "数据来源": "promotion_confirm_batch",
            "操作用户": "promotion_validator",
            "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "正常" if action != "CONFIRM_FAILED" else "慢查询",
            "pending": False,
            "abnormal": action == "CONFIRM_FAILED",
            "备注": detail,
        })

    def _result(
        self, batch: dict[str, Any], *, reused: bool,
        reentered: bool = False, issues: list[str] | None = None,
    ) -> dict[str, Any]:
        return {
            "ok": batch["status"] in ("确认通过", "已消费"),
            "reused": reused,
            "reentered": reentered,
            "batch_id": batch["id"],
            "批次编号": batch["批次编号"],
            "status": batch["status"],
            "幂等键": batch["幂等键"],
            "模型指纹": batch["模型指纹"],
            "契约指纹": batch["契约指纹"],
            "fields_count": len(batch.get("模型快照", {}).get("源字段", [])),
            "issues": issues or [],
            "message": "确认通过" if batch["status"] in ("确认通过", "已消费")
                       else batch.get("失败原因", "确认失败"),
        }
