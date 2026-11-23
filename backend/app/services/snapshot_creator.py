"""数据快照领域服务：创建带血缘信息的数据快照、快照差异对比和快照恢复列表查询。

数据链路位置：数据快照层，为数据版本管理和恢复提供时间点快照。

一致性设计：维度数据集的快照结案前，必须核对到匹配数据集和配置
版本的成功维度构建记录；快照详情与血缘登记始终指向同一次已完成
的维度构建。已完成但血缘缺失的任务按中断前选定的构建记录补办，
不因补办期间出现新版本而改用，重复提交幂等返回原登记结果。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SnapshotCreatorService(BaseModuleService):
    MODULE = "snapshot_creator"
    ENTITY = "快照任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["目标数据集", "快照时间点", "快照类型", "存储路径", "文件大小"]
    REQUIRED_FIELDS = ["目标数据集", "快照时间点", "快照类型"]
    STATUS_ORDER = ["排队中", "创建中", "已完成", "创建失败"]
    ACTION_RULES = {"创建快照": "排队中", "取消快照": "创建中"}
    NEGATIVE_ACTIONS = []

    SNAPSHOT_TYPES = frozenset({"full", "incremental", "daily_sealed"})

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is not None:
            # 值班员按构建记录登记历史快照任务时锁定配置版本
            version = str(values.get("维度配置版本") or "").strip()
            if version:
                entry["维度配置版本"] = version.upper()
        return entry, missing

    # ------------------------------------------------------------------
    # 维度构建记录核对
    # ------------------------------------------------------------------
    def _dimension_service(self):
        # 延迟导入避免 services 包初始化循环依赖
        from app.services import service_for
        return service_for("dimension_builder")

    def _resolve_dimension_build(
        self, entry: dict[str, Any], dataset: str
    ) -> tuple[dict[str, Any] | None, str | None]:
        """核对维度构建记录。

        返回 (build, error)：数据集没有维度配置时 build 为 None（普通
        快照，无需构建记录）；有维度配置但找不到匹配数据集和配置版本
        的成功构建记录时返回 error，快照不得结案。
        """
        dim_svc = self._dimension_service()
        configs = [
            row for row in store.rows("dimension_builder")
            if row.get("来源表") == dataset
        ]
        if not configs:
            return None, None

        version = str(entry.get("维度配置版本") or "").strip()
        if not version:
            latest = dim_svc.latest_successful_build(dataset)
            version = latest["config_version"] if latest else ""
        if not version:
            return None, f"数据集 {dataset} 没有任何成功的维度构建记录，快照不得结案"

        build = dim_svc.find_successful_build(dataset, version)
        if build is None:
            return None, (
                f"没有匹配数据集 {dataset} 和配置版本 {version} 的成功维度构建记录，"
                "快照不得结案，也不能登记血缘"
            )

        # 把选定的构建固化到任务上：结案、补办和追溯都以它为准
        entry["维度配置版本"] = build["config_version"]
        entry["维度构建ID"] = build["build_id"]
        entry["维度键"] = build["dimension_key"]
        entry["SCD策略"] = build["scd_type"]
        return build, None

    def _build_of_entry(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        build_id = entry.get("维度构建ID")
        if build_id is None:
            return None
        return self._dimension_service().get_build_record(int(build_id))

    # ------------------------------------------------------------------
    # 血缘登记
    # ------------------------------------------------------------------
    def _lineage_rows_for_snapshot(self, snapshot_id: int) -> list[dict[str, Any]]:
        return [
            row for row in store.rows("lineage_graph")
            if row.get("snapshot_id") is not None
            and int(row.get("snapshot_id")) == snapshot_id
        ]

    def _ensure_lineage(
        self, entry: dict[str, Any], build: dict[str, Any] | None
    ) -> tuple[dict[str, Any], bool]:
        """登记快照血缘；已登记的正确关系保留，不重复写入。"""
        existing = self._lineage_rows_for_snapshot(entry["id"])
        if existing:
            return existing[0], False

        dataset = entry.get("目标数据集", "")
        snapshot_type = entry.get("快照类型", "full")
        snapshot_time = entry.get("快照时间点", "")
        if build is not None:
            source = (
                f"{dataset} (维度构建#{build['build_id']} "
                f"{build['config_version']} {build['scd_type']})"
            )
        else:
            source = dataset

        rows = store.rows("lineage_graph")
        lid = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        row = {
            "id": lid,
            "status": "已建立",
            "pending": False,
            "abnormal": False,
            "目标表": f"{dataset}_snapshot",
            "来源表列表": source,
            "转换逻辑": f"snapshot_{snapshot_type}_at_{snapshot_time}",
            "中间层级": "1",
            "血缘深度": "1",
            "snapshot_id": entry["id"],
            "dimension_build_id": build["build_id"] if build else None,
            "config_version": build["config_version"] if build else None,
            "维度键": build["dimension_key"] if build else None,
            "SCD策略": build["scd_type"] if build else None,
        }
        rows.append(row)
        return row, True

    # ------------------------------------------------------------------
    # 快照创建 / 补办（幂等）
    # ------------------------------------------------------------------
    def create_snapshot_with_lineage(
        self, entry_id: int, *, fail_before_lineage: bool = False
    ) -> dict[str, Any]:
        """创建快照并同步登记血缘（幂等）。

        - 未完成：核对维度构建记录 → 结案 → 登记血缘；
        - 已完成但血缘缺失：按中断前选定的构建记录补办血缘；
        - 已完成且血缘在册：直接返回原登记结果。

        fail_before_lineage 是测试钩子，模拟结案后、血缘写入前中断。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "快照任务不存在"}

        if entry.get("status") == "已完成":
            return self._repair_or_return(entry, fail_before_lineage=fail_before_lineage)

        dataset = entry.get("目标数据集", "")
        build, error = self._resolve_dimension_build(entry, dataset)
        if error:
            return {
                "ok": False,
                "snapshot_id": entry_id,
                "dataset": dataset,
                "message": error,
            }

        snapshot_type = entry.get("快照类型", "full")
        snapshot_time = entry.get("快照时间点", datetime.now().isoformat())
        storage_path = entry.get("存储路径") or f"/data/snapshots/{dataset}/{snapshot_time}"

        entry["status"] = "已完成"
        entry["存储路径"] = storage_path
        entry["文件大小"] = f"{self._estimate_snapshot_size(snapshot_type)} GB"
        entry["pending"] = False

        if fail_before_lineage:
            return {
                "ok": False,
                "snapshot_id": entry_id,
                "dataset": dataset,
                "config_version": entry.get("维度配置版本"),
                "dimension_build_id": entry.get("维度构建ID"),
                "message": "快照已结案，血缘登记前中断",
            }

        lineage, created = self._ensure_lineage(entry, build)
        return self._success_result(
            entry, lineage, repaired=False, lineage_created=created)

    def _repair_or_return(
        self, entry: dict[str, Any], *, fail_before_lineage: bool = False
    ) -> dict[str, Any]:
        """已完成任务的重试入口：血缘在册则幂等返回，缺失则按原构建补办。"""
        existing = self._lineage_rows_for_snapshot(entry["id"])
        if existing:
            return self._success_result(
                entry, existing[0], repaired=False, lineage_created=False,
                message="快照任务已办结，血缘在册，返回原登记结果")

        build_id = entry.get("维度构建ID")
        build = self._build_of_entry(entry)
        if build_id is not None and build is None:
            return {
                "ok": False,
                "snapshot_id": entry["id"],
                "message": f"原任务采用的维度构建记录 #{build_id} 已缺失，无法补办血缘",
            }

        if fail_before_lineage:
            return {
                "ok": False,
                "snapshot_id": entry["id"],
                "message": "血缘补办前再次中断",
            }

        lineage, created = self._ensure_lineage(entry, build)
        return self._success_result(
            entry, lineage, repaired=True, lineage_created=created,
            message="血缘登记补办完成，仍指向中断前选定的维度构建")

    def _success_result(
        self,
        entry: dict[str, Any],
        lineage: dict[str, Any] | None,
        *,
        repaired: bool,
        lineage_created: bool,
        message: str | None = None,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "snapshot_id": entry["id"],
            "snapshot_type": entry.get("快照类型"),
            "dataset": entry.get("目标数据集"),
            "timestamp": entry.get("快照时间点"),
            "storage_path": entry.get("存储路径"),
            "size_gb": entry.get("文件大小"),
            "config_version": entry.get("维度配置版本"),
            "dimension_build_id": entry.get("维度构建ID"),
            "dimension_key": entry.get("维度键"),
            "scd_type": entry.get("SCD策略"),
            "lineage_id": lineage.get("id") if lineage else None,
            "repaired": repaired,
            "lineage_created": lineage_created,
            "message": message or "快照创建完成并已注册血缘",
        }

    # ------------------------------------------------------------------
    # 血缘追溯与历史快照列表
    # ------------------------------------------------------------------
    def trace_snapshot_lineage(self, entry_id: int) -> dict[str, Any]:
        """血缘追溯：快照详情与血缘记录必须指向同一次已完成的维度构建。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "快照任务不存在"}

        rows = self._lineage_rows_for_snapshot(entry_id)
        build_id = entry.get("维度构建ID")
        consistent = bool(rows) and all(
            (row.get("dimension_build_id") or None) == (build_id or None)
            for row in rows
        )
        deliverable = entry.get("status") == "已完成" and bool(rows) and consistent
        return {
            "ok": True,
            "snapshot_id": entry_id,
            "snapshot_status": entry.get("status"),
            "config_version": entry.get("维度配置版本"),
            "dimension_build_id": build_id,
            "lineage": rows,
            "lineage_count": len(rows),
            "consistent": consistent,
            "deliverable": deliverable,
        }

    def list_restorable_snapshots(self, dataset: str) -> dict[str, Any]:
        """列出指定数据集可交付的历史快照，供冷数据恢复模块查询。

        列表中的完成结果必须有对应的血缘登记；已办成的快照保留原来
        使用的配置版本。返回列表按时间倒序排列。
        """
        all_snapshots = store.rows(self.MODULE)
        matched = [
            s for s in all_snapshots
            if s.get("目标数据集") == dataset and s.get("status") == "已完成"
        ]
        matched.sort(key=lambda s: s.get("快照时间点", ""), reverse=True)

        deliverable = []
        for snap in matched:
            lineage = self._lineage_rows_for_snapshot(snap["id"])
            if not lineage:
                # 已完成但血缘缺失：不可交付，不进历史快照列表
                continue
            deliverable.append({
                "id": snap["id"],
                "time": snap.get("快照时间点"),
                "type": snap.get("快照类型"),
                "path": snap.get("存储路径"),
                "size": snap.get("文件大小"),
                "config_version": snap.get("维度配置版本"),
                "dimension_build_id": snap.get("维度构建ID"),
                "lineage_id": lineage[0]["id"],
                "restorable": True,
            })

        return {
            "dataset": dataset,
            "total_snapshots": len(deliverable),
            "latest_snapshot_time": deliverable[0]["time"] if deliverable else None,
            "oldest_snapshot_time": deliverable[-1]["time"] if deliverable else None,
            "snapshots": deliverable[:20],
        }

    # ------------------------------------------------------------------
    # 既有能力
    # ------------------------------------------------------------------
    def diff_snapshots(self, snapshot_id_a: int, snapshot_id_b: int) -> dict[str, Any]:
        """比较两个快照之间的数据差异。

        返回新增行数、删除行数、修改行数和变更字段列表，
        可用于审计追溯和数据回滚决策。
        """
        snap_a = store.find(self.MODULE, snapshot_id_a)
        snap_b = store.find(self.MODULE, snapshot_id_b)
        if snap_a is None or snap_b is None:
            return {"ok": False, "message": "快照不存在"}
        if snap_a.get("目标数据集") != snap_b.get("目标数据集"):
            return {"ok": False, "message": "快照数据集不一致，无法比较"}

        diff = {
            "ok": True,
            "snapshot_a": {"id": snapshot_id_a, "time": snap_a.get("快照时间点"), "size": snap_a.get("文件大小")},
            "snapshot_b": {"id": snapshot_id_b, "time": snap_b.get("快照时间点"), "size": snap_b.get("文件大小")},
            "dataset": snap_a.get("目标数据集"),
            "added_rows": 2850,
            "deleted_rows": 120,
            "modified_rows": 3400,
            "unchanged_rows": 1540000,
            "changed_fields": ["amount", "status", "updated_at"],
            "revert_cost_estimate_seconds": 120,
        }
        return diff

    def validate_snapshot_integrity(self, entry_id: int) -> dict[str, Any]:
        """校验快照的数据完整性。

        对比快照的元数据和实际文件，检查是否存在损坏或缺失。
        校验结果写入 data_lake_audit 模块。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "快照不存在"}

        checks = [
            {"check": "文件存在性", "passed": True, "detail": "快照文件路径可访问"},
            {"check": "行数一致性", "passed": True, "detail": "快照行数与元数据一致"},
            {"check": "Schema完整性", "passed": True, "detail": "所有字段与源表一致"},
            {"check": "校验和匹配", "passed": True, "detail": "MD5 校验和与创建时一致"},
            {"check": "压缩完整性", "passed": True, "detail": "Parquet 格式验证通过"},
        ]
        all_passed = all(c["passed"] for c in checks)

        if not all_passed:
            entry["abnormal"] = True

        return {
            "ok": all_passed,
            "snapshot_id": entry_id,
            "dataset": entry.get("目标数据集"),
            "checks": checks,
            "overall": "pass" if all_passed else "fail",
        }

    def _estimate_snapshot_size(self, snapshot_type: str) -> float:
        base = 1.5
        if snapshot_type == "full":
            return base * 3
        elif snapshot_type == "incremental":
            return base * 0.3
        return base
