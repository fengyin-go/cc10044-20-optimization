"""领域操作接口：维度增量构建、构建记录回查、快照提交与血缘追溯。

这些端点挂在通用资源路由之前注册，固定段（plan/build/builds/
submit/lineage/restorable）不会被 ``/{module}/{entry_id}`` 抢占。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services import service_for

router = APIRouter(prefix="/api", tags=["领域操作"])


def _dimension_service():
    service = service_for("dimension_builder")
    if service is None:
        raise HTTPException(status_code=404, detail="维度表生成器模块未注册")
    return service


def _snapshot_service():
    service = service_for("snapshot_creator")
    if service is None:
        raise HTTPException(status_code=404, detail="快照生成器模块未注册")
    return service


@router.get("/dimension_builder/{entry_id}/plan")
def dimension_plan(entry_id: int) -> dict[str, Any]:
    """生成维度构建计划（只演算，不产生构建记录）。"""
    return _dimension_service().generate_dimension_plan(entry_id)


@router.post("/dimension_builder/{entry_id}/build")
def build_dimension(entry_id: int) -> dict[str, Any]:
    """发起增量构建；仅已上线配置可构建，成功后固化构建记录。"""
    return _dimension_service().build_dimension_incremental(entry_id)


@router.get("/dimension_builder/{entry_id}/builds")
def dimension_build_records(entry_id: int) -> dict[str, Any]:
    """回查该配置的维度构建记录：换版后仍能查到历史构建用的版本。"""
    service = _dimension_service()
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"维度配置 {entry_id} 不存在或已归档")
    records = service.list_build_records(config_id=entry_id)
    return {
        "items": records,
        "total": len(records),
        "config_version": service.config_version_of(entry),
    }


@router.post("/snapshot_creator/{entry_id}/submit")
def submit_snapshot(entry_id: int) -> dict[str, Any]:
    """提交快照任务：核对维度构建记录后结案并登记血缘（幂等）。"""
    return _snapshot_service().create_snapshot_with_lineage(entry_id)


@router.get("/snapshot_creator/{entry_id}/lineage")
def snapshot_lineage(entry_id: int) -> dict[str, Any]:
    """血缘追溯：快照详情与血缘记录须指向同一次已完成的维度构建。"""
    return _snapshot_service().trace_snapshot_lineage(entry_id)


@router.get("/snapshot_creator/restorable")
def restorable_snapshots(dataset: str = Query(...)) -> dict[str, Any]:
    """历史快照列表：只列出已完成且血缘登记在册的可交付快照。"""
    return _snapshot_service().list_restorable_snapshots(dataset)
