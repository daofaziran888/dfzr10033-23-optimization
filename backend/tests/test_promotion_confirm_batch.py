"""投产映射确认批次：幂等、重入、契约切换防争用、约束版本与发布清单、并发对账。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store
from app.services.promotion_confirm_batch import PromotionConfirmBatchService
from app.services.semantic_model import SemanticModelService
from app.services.data_contract import DataContractService

client = TestClient(app)


def _make_model(suffix: str, *, aliases: str = "id,user_id,amount",
                measures: str = "COUNT(*),SUM(amount)", source_id: int = 1,
                version: str = "v1.0") -> dict:
    rows = store.rows("semantic_model")
    model = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "模型编号": f"SEMA-T-{suffix}",
        "模型名称": f"投产测试模型_{suffix}",
        "源表映射": f"source_registry:{source_id}",
        "字段别名": aliases,
        "度量定义": measures,
        "维度定义": "region,category",
        "计算口径": "标准口径",
        "版本序号": version,
        "status": "草稿",
        "pending": True,
        "abnormal": False,
    }
    rows.append(model)
    return model


def _make_contract(suffix: str, *, schema: str = "id,user_id,amount",
                   dataset: str | None = None, status: str = "草稿") -> dict:
    rows = store.rows("data_contract")
    contract = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "契约编号": f"DATA-T-{suffix}",
        "数据集": dataset or f"ds_{suffix}",
        "Schema约束": schema,
        "SLA定义": "每小时更新",
        "更新频率": "3600",
        "负责人": "tester",
        "消费者": "dashboard",
        "生效时间": "2026-11-21",
        "status": status,
        "pending": True,
        "abnormal": False,
    }
    rows.append(contract)
    return contract


@pytest.fixture
def services():
    return {
        "batch": PromotionConfirmBatchService(),
        "semantic": SemanticModelService(),
        "contract": DataContractService(),
    }


# 全局 store 是进程内单例，用例之间会累积行；这里在每个用例前后快照还原，
# 保证测试可独立、任意顺序运行。
_ISOLATED_TABLES = [
    "semantic_model", "data_contract", "promotion_confirm_batch",
    "contract_constraint_version", "contract_field_change",
    "result_mart", "query_audit",
]


@pytest.fixture(autouse=True)
def isolate_store():
    backup = {
        name: [dict(row) for row in store.rows(name)]
        for name in _ISOLATED_TABLES
    }
    yield
    for name, rows in backup.items():
        store._tables[name] = [dict(row) for row in rows]


class TestPromotionConfirmBatch:
    def test_submit_snapshots_full_mapping_and_promotes(self, services):
        """一次读取同时拿到源字段、别名、聚合定义，确认后模型原子投产。"""
        model = _make_model("A1")
        contract = _make_contract("A1")
        result = services["batch"].submit_confirmation(model["id"], contract["id"])

        assert result["ok"] and result["status"] == "确认通过"
        assert result["模型指纹"] and result["契约指纹"]
        assert result["fields_count"] == 3

        stored = store.find("promotion_confirm_batch", result["batch_id"])
        snap = stored["模型快照"]
        assert snap["源字段"] == ["id", "user_id", "amount"]
        assert snap["字段别名"] == ["id", "user_id", "amount"]
        assert snap["聚合定义"] == ["COUNT(*)", "SUM(amount)"]
        assert snap["源登记"] and snap["源登记"][0]["id"] == 1
        assert stored["契约快照"]["契约列"] == ["id", "user_id", "amount"]

        refreshed = store.find("semantic_model", model["id"])
        assert refreshed["status"] == "已投产"
        assert refreshed["投产确认批次"] == stored["批次编号"]
        assert refreshed["投产指纹"] == result["模型指纹"]

    def test_duplicate_submit_keeps_single_confirmation(self, services):
        """重复提交同一校验批只保留一个确认结果，第二次直接回放。"""
        model = _make_model("A2")
        contract = _make_contract("A2")
        first = services["batch"].submit_confirmation(model["id"], contract["id"])
        second = services["batch"].submit_confirmation(model["id"], contract["id"])
        third = services["batch"].submit_confirmation(model["id"], contract["id"])

        assert first["batch_id"] == second["batch_id"] == third["batch_id"]
        assert second["reused"] and third["reused"]
        batches = [b for b in store.rows("promotion_confirm_batch")
                   if b["幂等键"] == first["幂等键"]]
        assert len(batches) == 1
        # 回放不应重复写审计
        audits = [a for a in store.rows("query_audit")
                  if first["批次编号"] in str(a.get("查询语句", ""))]
        assert len(audits) == 1

    def test_failed_batch_reenters_without_half_update(self, services):
        """校验失败不落半更新定义，修复后重新进入同批续跑并通过。"""
        model = _make_model("A3", measures="")
        contract = _make_contract("A3")
        failed = services["batch"].submit_confirmation(model["id"], contract["id"])
        assert not failed["ok"] and failed["status"] == "确认失败"
        assert "聚合" in failed["issues"][0]

        # 失败不得改动模型 / 契约定义
        assert store.find("semantic_model", model["id"])["status"] == "草稿"
        assert "确认批次编号" not in store.find("data_contract", contract["id"])

        # 修复后重新进入同一批次（id 不变），而不是新建确认结果
        model["度量定义"] = "SUM(amount)"
        reenter = services["batch"].reenter_confirmation(failed["batch_id"])
        assert reenter["ok"] and reenter["reentered"]
        assert reenter["batch_id"] == failed["batch_id"]
        stored = store.find("promotion_confirm_batch", failed["batch_id"])
        assert stored["status"] == "确认通过" and stored["重入次数"] == 1
        assert store.find("semantic_model", model["id"])["status"] == "已投产"

    def test_reenter_non_failed_batch_rejected(self, services):
        model = _make_model("A4")
        contract = _make_contract("A4")
        passed = services["batch"].submit_confirmation(model["id"], contract["id"])
        again = services["batch"].reenter_confirmation(passed["batch_id"])
        assert not again["ok"] and "重新进入" in again["message"]

    def test_contract_switch_creates_separate_batch_and_blocks_stale_consume(self, services):
        """一次投产读完字段后契约换成另一套列定义：两份口径各自留痕，旧批不能消费新契约。"""
        model = _make_model("A5")
        contract = _make_contract("A5", schema="id,user_id,amount")
        first = services["batch"].submit_confirmation(model["id"], contract["id"])
        assert first["ok"]

        # 契约切换列定义（走字段变更记录，不覆盖模型字段）
        change = services["contract"].propagate_schema_change(
            contract["id"], "id,user_id,amount,region,channel")
        assert change["ok"]
        model_row = store.find("semantic_model", model["id"])
        assert model_row["字段别名"] == "id,user_id,amount"  # 模型未被原地改写
        assert model_row.get("待确认契约变更") == change["change_id"]
        assert store.find("data_contract", contract["id"])["当前约束版本"] is None

        # 新口径产生新的确认批，历史批次仍完整可查
        second = services["batch"].submit_confirmation(model["id"], contract["id"])
        assert second["ok"] and second["batch_id"] != first["batch_id"]
        assert second["幂等键"] != first["幂等键"]
        old_batch = store.find("promotion_confirm_batch", first["batch_id"])
        assert old_batch["契约快照"]["契约列"] == ["id", "user_id", "amount"]

        # 旧批次与当前契约指纹不符，消费必须被漂移护栏拦下
        stale = services["contract"].consume_confirmation_batch(first["batch_id"])
        assert not stale["ok"] and "偏离" in stale["message"]

        # 新批次可以正常消费
        consumed = services["contract"].consume_confirmation_batch(second["batch_id"])
        assert consumed["ok"]

    def test_consume_writes_verifiable_constraint_and_manifest(self, services):
        """契约消费真实模型映射，生成可校验约束版本并回写发布清单。"""
        model = _make_model("A6")
        contract = _make_contract("A6")
        confirmed = services["batch"].submit_confirmation(model["id"], contract["id"])
        consumed = services["contract"].consume_confirmation_batch(confirmed["batch_id"])

        assert consumed["ok"] and consumed["约束版本"] == "cv1.0"
        assert set(consumed["约束字段"]) == {"id", "user_id", "amount", "COUNT(*)",
                                             "SUM(amount)", "region", "category"}
        assert consumed["约束哈希"]

        versions = store.rows("contract_constraint_version")
        version_row = next(v for v in versions if v["id"] == consumed["约束版本ID"])
        assert version_row["模型指纹"] == confirmed["模型指纹"]
        assert version_row["status"] == "生效中"

        contract_row = store.find("data_contract", contract["id"])
        assert contract_row["当前约束版本"] == "cv1.0"
        assert contract_row["约束哈希"] == consumed["约束哈希"]
        assert contract_row["确认批次编号"] == confirmed["批次编号"]

        manifest = store.find("result_mart", consumed["发布清单ID"])
        assert manifest is not None
        assert manifest["确认批次编号"] == confirmed["批次编号"]
        assert manifest["约束版本"] == "cv1.0" and manifest["status"] == "可用"

        batch_row = store.find("promotion_confirm_batch", confirmed["batch_id"])
        assert batch_row["status"] == "已消费"

    def test_consume_is_idempotent_and_archives_old_versions(self, services):
        model = _make_model("A7")
        contract = _make_contract("A7")
        first = services["batch"].submit_confirmation(model["id"], contract["id"])
        c1 = services["contract"].consume_confirmation_batch(first["batch_id"])

        # 模型改版后新批消费，旧版本归档，重复消费只返回既有版本
        model["版本序号"] = "v2.0"
        model["字段别名"] = "id,user_id,amount,coupon"
        second = services["batch"].submit_confirmation(model["id"], contract["id"])
        c2 = services["contract"].consume_confirmation_batch(second["batch_id"])
        assert c2["约束版本"] == "cv2.0"

        versions = [v for v in store.rows("contract_constraint_version")
                    if int(v["契约ID"]) == contract["id"]]
        assert {v["约束版本"]: v["status"] for v in versions} == {
            "cv1.0": "已归档", "cv2.0": "生效中"}

        replay = services["contract"].consume_confirmation_batch(first["batch_id"])
        assert replay["ok"] and replay["reused"] and replay["约束版本"] == "cv1.0"
        assert len([v for v in store.rows("contract_constraint_version")
                    if int(v["批次ID"]) == first["batch_id"]]) == 1

    def test_history_batch_restores_confirmed_combination(self, services):
        """可以从历史批次还原当时确认的完整映射 × 契约组合。"""
        model = _make_model("A8", version="v1.3")
        contract = _make_contract("A8", schema="id,user_id,amount")
        confirmed = services["batch"].submit_confirmation(model["id"], contract["id"])

        # 之后模型与契约都发生变化
        model["字段别名"] = "id,user_id,amount,new_col"
        model["版本序号"] = "v2.0"
        services["contract"].propagate_schema_change(contract["id"], "id,user_id,amount,new_col")

        detail = services["batch"].get_batch_detail(confirmed["batch_id"])
        assert detail["ok"]
        old_snap = detail["batch"]["模型快照"]
        old_contract = detail["batch"]["契约快照"]
        assert old_snap["版本序号"] == "v1.3"
        assert old_snap["源字段"] == ["id", "user_id", "amount"]
        assert old_contract["契约列"] == ["id", "user_id", "amount"]

        resp = client.get(f"/api/pipelines/promotion-confirm/{confirmed['batch_id']}")
        assert resp.status_code == 200 and resp.json()["batch"]["批次编号"] == confirmed["批次编号"]

    def test_reconcile_detects_and_clears_drift(self, services):
        """并发操作后刷新模型和契约，版本/字段/口径对得上才算 aligned。"""
        model = _make_model("A9")
        contract = _make_contract("A9")
        confirmed = services["batch"].submit_confirmation(model["id"], contract["id"])
        services["contract"].consume_confirmation_batch(confirmed["batch_id"])

        aligned = services["batch"].refresh_and_reconcile(model["id"], contract["id"])
        assert aligned["ok"] and aligned["aligned"]
        assert aligned["active_batch"]["批次编号"] == confirmed["批次编号"]
        assert aligned["release_manifest"] is not None

        # 契约切换列定义后立即对账：应报口径漂移
        services["contract"].propagate_schema_change(contract["id"], "id,user_id,amount,x,y")
        drifted = services["batch"].refresh_and_reconcile(model["id"], contract["id"])
        assert not drifted["aligned"]
        assert any("契约列定义已切换" in m for m in drifted["mismatches"])

        # 重新确认并消费后恢复对齐
        new_batch = services["batch"].submit_confirmation(model["id"], contract["id"])
        services["contract"].consume_confirmation_batch(new_batch["batch_id"])
        realigned = services["batch"].refresh_and_reconcile(model["id"], contract["id"])
        assert realigned["aligned"]

    def test_concurrent_submissions_yield_one_batch_no_half_update(self, services):
        """并发重复提交：同一把锁 + 幂等键保证只有一个确认结果。"""
        model = _make_model("A10")
        contract = _make_contract("A10")
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(
                lambda _: services["batch"].submit_confirmation(model["id"], contract["id"]),
                range(8),
            ))
        assert all(r["ok"] for r in results)
        assert len({r["batch_id"] for r in results}) == 1
        pair_batches = [b for b in store.rows("promotion_confirm_batch")
                        if int(b["关联模型"]) == model["id"]
                        and int(b["关联契约"]) == contract["id"]]
        assert len(pair_batches) == 1
        assert pair_batches[0]["status"] == "确认通过"
        assert store.find("semantic_model", model["id"])["status"] == "已投产"

    def test_legacy_model_and_contract_compatibility(self, services):
        """兼容历史模型版本（无版本序号）与老契约约束（无约束版本字段）。"""
        legacy_model = _make_model("A11", version="")
        legacy_model["版本序号"] = ""
        legacy_contract = _make_contract("A11")
        legacy_contract.pop("当前约束版本", None)

        result = services["batch"].submit_confirmation(legacy_model["id"], legacy_contract["id"])
        assert result["ok"]
        detail = services["batch"].get_batch_detail(result["batch_id"])
        assert detail["batch"]["模型快照"]["版本序号"].startswith("unversioned-")
        assert detail["batch"]["契约快照"]["版本"].startswith("v0-")

        # 旧入口 validate_and_promote 不传 contract_id 也能按数据集/名称配对
        legacy_model_2 = _make_model("A12")
        legacy_contract_2 = _make_contract(
            "A12", dataset=legacy_model_2["模型名称"])
        legacy_result = services["semantic"].validate_and_promote(legacy_model_2["id"])
        assert legacy_result["ok"] and legacy_result["status"] == "已投产"
        assert store.find("semantic_model", legacy_model_2["id"])["投产指纹"]

    def test_api_endpoints_end_to_end(self):
        """HTTP 层：提交 / 重复提交回放 / 消费 / 对账 全链路。"""
        model = _make_model("B1")
        contract = _make_contract("B1")

        resp = client.post("/api/pipelines/promotion-confirm/submit",
                           params={"model_id": model["id"], "contract_id": contract["id"]})
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"]
        batch_id = body["batch_id"]

        replay = client.post("/api/pipelines/promotion-confirm/submit",
                             params={"model_id": model["id"], "contract_id": contract["id"]})
        assert replay.json()["reused"] is True

        consume = client.post(f"/api/pipelines/promotion-confirm/{batch_id}/consume")
        assert consume.status_code == 200 and consume.json()["ok"]

        reconcile = client.get("/api/pipelines/promotion-confirm/reconcile",
                               params={"model_id": model["id"], "contract_id": contract["id"]})
        assert reconcile.status_code == 200 and reconcile.json()["aligned"] is True

        reenter = client.post(f"/api/pipelines/promotion-confirm/{batch_id}/reenter")
        assert reenter.status_code == 200 and reenter.json()["ok"] is False
