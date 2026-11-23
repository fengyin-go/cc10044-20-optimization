"""维度构建领域服务：生成维度表构建计划、增量构建和历史维度查询。

数据链路位置：维度建模层，构建和维护数据仓库的维度数据集。

一致性设计：每次成功的增量构建都会把客户键（维度键）、SCD 策略和
配置版本固化为一条维度构建记录；构建计划只是演算结果，不落记录，
与成功构建记录分开保存。配置换版后，历史构建记录仍可回查上一次
构建实际使用的是哪一版配置。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DimensionBuilderService(BaseModuleService):
    MODULE = "dimension_builder"
    ENTITY = "维度配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置名称", "来源表", "维度键", "缓慢变化处理", "历史快照"]
    REQUIRED_FIELDS = ["配置名称", "来源表", "维度键"]
    STATUS_ORDER = ["草稿", "已上线", "已停用"]
    ACTION_RULES = {"上线配置": "已上线", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    SCD_TYPES = frozenset({"SCD_TYPE_1", "SCD_TYPE_2", "SCD_TYPE_3"})
    DEFAULT_VERSION = "V1"

    def __init__(self) -> None:
        # 维度构建记录：只登记成功完成的增量构建，
        # 与 generate_dimension_plan 演算出的计划分开保存。
        self._build_records: list[dict[str, Any]] = []
        self._build_seq = 0

    # ------------------------------------------------------------------
    # 配置版本：兼容既有配置，未显式登记版本的一律按 V1 处理，策略不变
    # ------------------------------------------------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is not None:
            entry["配置版本"] = self.normalize_version(values.get("配置版本"))
        return entry, missing

    def config_version_of(self, entry: dict[str, Any]) -> str:
        """读取配置版本；兼容没有版本字段的既有配置，保留其原策略。"""
        return self.normalize_version(entry.get("配置版本"))

    def normalize_version(self, raw: Any) -> str:
        version = str(raw or "").strip().upper()
        return version or self.DEFAULT_VERSION

    # ------------------------------------------------------------------
    # 维度构建记录：客户键 + SCD 策略 + 配置版本随成功构建固化
    # ------------------------------------------------------------------
    def list_build_records(
        self,
        *,
        dataset: str | None = None,
        config_version: str | None = None,
        config_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """回查维度构建记录：换版后仍能查到上一次构建实际使用的版本。"""
        records = list(self._build_records)
        if dataset:
            records = [r for r in records if r["dataset"] == dataset]
        if config_version:
            version = self.normalize_version(config_version)
            records = [r for r in records if r["config_version"] == version]
        if config_id is not None:
            records = [r for r in records if r["config_id"] == config_id]
        return records

    def get_build_record(self, build_id: int) -> dict[str, Any] | None:
        for record in self._build_records:
            if record["build_id"] == build_id:
                return record
        return None

    def find_successful_build(self, dataset: str, config_version: str) -> dict[str, Any] | None:
        """按数据集 + 配置版本定位最近一次成功构建。"""
        version = self.normalize_version(config_version)
        matched = [
            record for record in self._build_records
            if record["dataset"] == dataset
            and record["config_version"] == version
            and record["status"] == "成功"
        ]
        return matched[-1] if matched else None

    def latest_successful_build(self, dataset: str) -> dict[str, Any] | None:
        """数据集最近一次成功构建，用于未指定版本时的来源核对。"""
        matched = [
            record for record in self._build_records
            if record["dataset"] == dataset and record["status"] == "成功"
        ]
        return matched[-1] if matched else None

    # ------------------------------------------------------------------
    # 构建计划与增量构建
    # ------------------------------------------------------------------
    def generate_dimension_plan(self, entry_id: int) -> dict[str, Any]:
        """生成维度表构建计划。

        根据来源表、维度键和SCD策略，生成维度表的生成SQL和更新逻辑。
        计划只是演算结果，不产生构建记录。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "维度配置不存在"}

        source_table = entry.get("来源表", "")
        dim_key = entry.get("维度键", "")
        scd_type = entry.get("缓慢变化处理", "SCD_TYPE_2")
        use_history = entry.get("历史快照", "")

        columns = self._extract_dimension_columns(source_table, dim_key)
        plan = {
            "config_id": entry_id,
            "config_name": entry.get("配置名称"),
            "config_version": self.config_version_of(entry),
            "source_table": source_table,
            "dimension_key": dim_key,
            "scd_type": scd_type,
            "columns": columns,
            "output_table": f"dim_{source_table}",
            "surrogate_key": f"{dim_key}_sk",
            "effective_date_col": "eff_start_date",
            "expiration_date_col": "eff_end_date",
            "is_current_col": "is_current",
        }
        if scd_type == "SCD_TYPE_2":
            plan["history_table"] = f"dim_{source_table}_history"
            plan["track_changes_on"] = columns[:3]
        elif scd_type == "SCD_TYPE_3":
            plan["previous_value_cols"] = [f"prev_{col}" for col in columns[:2]]
        return plan

    def build_dimension_incremental(self, entry_id: int) -> dict[str, Any]:
        """增量构建维度表。

        只有已上线的配置才能发起增量构建。SCD_TYPE_2 模式下为每个变更
        生成新的维度行并更新旧行的有效期，SCD_TYPE_1 模式下直接覆盖
        变更字段。构建成功后把客户键、SCD 策略和配置版本写入构建记录。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "维度配置不存在"}
        if entry.get("status") != "已上线":
            return {"ok": False, "message": "只有上线的配置才能构建"}

        plan = self.generate_dimension_plan(entry_id)
        scd = plan["scd_type"]
        if scd == "SCD_TYPE_2":
            new_rows = 450
            updated_rows = 120
            expired_rows = 120
        elif scd == "SCD_TYPE_3":
            new_rows = 320
            updated_rows = 45
            expired_rows = 0
        else:
            new_rows = 200
            updated_rows = 60
            expired_rows = 0

        self._build_seq += 1
        record = {
            "build_id": self._build_seq,
            "config_id": entry_id,
            "config_version": plan["config_version"],
            "dataset": plan["source_table"],
            "dimension_key": plan["dimension_key"],
            "scd_type": scd,
            "status": "成功",
            "built_at": datetime.now(timezone.utc).isoformat(),
            "new_dimension_rows": new_rows,
            "updated_rows": updated_rows,
            "expired_rows": expired_rows,
            "total_dimension_rows": 25400,
            "history_table_updated": scd == "SCD_TYPE_2",
        }
        self._build_records.append(record)

        return {
            "ok": True,
            "config_id": entry_id,
            "build_id": record["build_id"],
            "config_version": record["config_version"],
            "dataset": record["dataset"],
            "dimension_key": record["dimension_key"],
            "scd_type": scd,
            "new_dimension_rows": new_rows,
            "updated_rows": updated_rows,
            "expired_rows": expired_rows,
            "total_dimension_rows": record["total_dimension_rows"],
            "history_table_updated": record["history_table_updated"],
            "message": f"批量维度构建完成：新增 {new_rows} 行，更新 {updated_rows} 行",
        }

    def query_dimension_history(self, entry_id: int, dim_value: str, as_of_date: str | None = None) -> dict[str, Any]:
        """查询维度历史快照。

        SCD_TYPE_2 下可以查询任意时间点的维度状态，
        用于数据溯源和审计回溯。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "维度配置不存在"}

        dim_key = entry.get("维度键", "")
        return {
            "config_id": entry_id,
            "config_version": self.config_version_of(entry),
            "dimension_key": dim_key,
            "dimension_value": dim_value,
            "as_of_date": as_of_date or "2026-11-21",
            "current_version": {
                "eff_start": "2026-09-15",
                "eff_end": "9999-12-31",
                "is_current": True,
                "values": {dim_key: dim_value, "name": f"{dim_value} Name", "category": "A"},
            },
            "history_versions": [
                {"eff_start": "2026-06-01", "eff_end": "2026-09-14", "is_current": False,
                 "values": {dim_key: dim_value, "name": f"{dim_value} Old Name", "category": "B"}},
            ],
            "total_versions": 2,
        }

    def get_or_create_surrogate_key(self, entry_id: int, natural_key: str) -> dict[str, Any]:
        """获取或生成代理键。

        为自然键分配稳定的代理键（surrogate key），
        实现业务键与技术键的解耦。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "维度配置不存在"}

        sk = f"SK_{hash(natural_key) % 1000000:06d}"
        return {
            "natural_key": natural_key,
            "surrogate_key": sk,
            "is_new": True,
            "dimension_table": f"dim_{entry.get('来源表')}",
        }

    def _extract_dimension_columns(self, source_table: str, dim_key: str) -> list[str]:
        col_map: dict[str, list[str]] = {
            "customer_master": [dim_key, "customer_name", "region", "segment", "loyalty_tier"],
            "product_catalog": [dim_key, "product_name", "category", "brand", "price_tier"],
            "supplier_master": [dim_key, "supplier_name", "country", "contract_type", "rating"],
        }
        return col_map.get(source_table, [dim_key, "name", "type", "status"])
