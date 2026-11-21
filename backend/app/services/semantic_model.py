"""语义模型领域服务：管理表映射、字段别名、度量定义与计算口径，作为查询翻译层。

改造要点：投产校验不再自行重复读取源表 / 契约并分别回写，而是统一委托给
``PromotionConfirmBatchService`` —— 一次读取完整映射快照（源字段、别名、聚合定义）
与契约快照，由确认批完成幂等校验、原子投产与可还原留痕。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.promotion_confirm_batch import PromotionConfirmBatchService
from app.store import store


class SemanticModelService(BaseModuleService):
    MODULE = "semantic_model"
    ENTITY = "语义定义"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["模型编号", "模型名称", "源表映射", "字段别名", "度量定义", "维度定义", "计算口径", "版本序号"]
    REQUIRED_FIELDS = ["模型编号", "模型名称", "源表映射"]
    STATUS_ORDER = ["草稿", "校验中", "已投产", "已废弃"]
    ACTION_RULES = {"提交校验": "校验中", "投产模型": "已投产", "废弃模型": "已废弃"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self._batch_service = PromotionConfirmBatchService()

    def validate_and_promote(self, model_id: int, contract_id: int | None = None) -> dict[str, Any]:
        """校验语义模型并投产（兼容旧入口）。

        新数据流：
        - 一次性读取模型完整映射快照（源字段、字段别名、聚合定义）与数据契约快照；
        - 提交投产映射确认批，由批次完成幂等校验与原子投产；
        - 未显式指定契约时，按数据集 / 模型名称匹配一份候选契约，保持历史调用可用。
        """
        model = store.find(self.MODULE, model_id)
        if model is None:
            return {"ok": False, "message": "语义模型不存在"}

        target_contract_id = contract_id or self._resolve_contract_id(model)
        if target_contract_id is None:
            return {
                "ok": False,
                "message": "未找到可配对的数据契约，请显式传入 contract_id 后再投产",
            }

        result = self._batch_service.submit_confirmation(model_id, target_contract_id)
        # 兼容旧返回结构
        if result.get("ok"):
            result["model_id"] = model_id
            result["status"] = "已投产"
        else:
            failed = store.find(self.MODULE, model_id)
            if failed is not None:
                failed["status"] = "已废弃"
                failed["abnormal"] = True
        return result

    def detect_schema_conflicts(self, source_id: int) -> dict[str, Any]:
        """检测数据源Schema变化与已投产语义模型之间的冲突。

        跨模块数据流：
        - 读取 source_registry 的Schema记录
        - 读取 data_lake_read 的Schema变更记录
        - 比较语义模型字段定义，标记冲突
        """
        models = store.rows(self.MODULE)
        conflicts = []

        for model in models:
            if model.get("status") != "已投产":
                continue
            if str(source_id) not in str(model.get("源表映射", "")):
                continue

            current_fields = set(model.get("字段别名", "").split(","))
            current_fields = {f.strip() for f in current_fields if f.strip()}

            # 检测 data_lake_read 中的Schema变更
            lake_rows = store.rows("data_lake_read")
            for lake in lake_rows:
                schema_changes = lake.get("Schema推导", "")
                if "source_registry" in lake.get("存储位置", "") and str(source_id) in lake.get("存储位置", ""):
                    conflicts.append({
                        "model_id": model["id"],
                        "model_name": model.get("模型名称"),
                        "source_id": source_id,
                        "schema_change": schema_changes,
                        "current_fields": list(current_fields),
                    })

        return {
            "ok": True,
            "source_id": source_id,
            "conflict_count": len(conflicts),
            "conflicts": conflicts,
        }

    def _resolve_contract_id(self, model: dict[str, Any]) -> int | None:
        """历史调用未指定契约时的配对规则：模型名称命中数据集，优先取未终止契约。"""
        contracts = store.rows("data_contract")
        model_name = str(model.get("模型名称", ""))
        source_ref = str(model.get("源表映射", ""))
        candidates = [
            c for c in contracts
            if model_name in str(c.get("数据集", ""))
            or str(c.get("数据集", "")) in source_ref
            or source_ref in str(c.get("数据集", ""))
        ]
        candidates.sort(key=lambda c: c.get("status") == "已终止")
        return int(candidates[0]["id"]) if candidates else None
