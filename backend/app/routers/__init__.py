"""业务路由汇总：领域操作路由固定在通用资源路由之前注册，
避免固定路径被 ``/{module}/{entry_id}`` 动态段抢占。"""
from __future__ import annotations

from app.routers import domain, resource

ROUTERS = [domain, resource]
