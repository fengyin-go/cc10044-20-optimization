"""维度构建记录与快照血缘一致性验收脚本。

场景一：同一客户数据集 V1(SCD 2)/V2(SCD 3) 双版本并存，按 V1 构建
        记录登记的历史快照可直接结案、无需重选版本；仅生成计划的
        配置不能产出已完成快照。
场景二：结案后、血缘写入前中断的任务不可交付；期间完成新版构建后
        重试，补办的血缘仍指向中断前选定的构建；重复提交幂等。

运行：.venv/bin/python -I tests/test_dimension_snapshot_consistency.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402

dim = service_for("dimension_builder")
snap = service_for("snapshot_creator")

checks = 0


def check(name, cond, detail=""):
    global checks
    assert cond, f"FAIL: {name} {detail}"
    checks += 1
    print(f"  ✓ {name}")


print("场景一：双版本并存，按 V1 构建记录登记历史快照")

# 既有配置兼容：seed 中的旧配置保留原版本号 V1 和原策略
seed_cfg = store.find("dimension_builder", 1)
check("既有配置保留原版本号 V1", dim.config_version_of(seed_cfg) == "V1")
check("既有配置保留原 SCD 策略", seed_cfg["缓慢变化处理"] == "SCD_TYPE_2")

# V1 已上线，完成一次增量构建，产生 V1 构建记录
r1 = dim.build_dimension_incremental(1)
check("V1 增量构建成功", r1["ok"] and r1["config_version"] == "V1")
check("构建记录固化客户键与 SCD 策略",
      r1["dimension_key"] == "customer_id" and r1["scd_type"] == "SCD_TYPE_2")
v1_build_id = r1["build_id"]

# 草稿配置不能发起增量构建
r_draft = dim.build_dimension_incremental(2)
check("只有已上线配置才能构建", not r_draft["ok"])

# 登记 V2（SCD 3），上线并完成增量构建
v2_entry, missing = dim.create_entry({
    "配置名称": "dim_customer_scd3", "来源表": "customer_master",
    "维度键": "customer_id", "缓慢变化处理": "SCD_TYPE_3",
    "历史快照": "", "配置版本": "V2",
})
check("V2 配置登记成功", v2_entry is not None and not missing)
check("V2 保留指定版本号", v2_entry["配置版本"] == "V2")
dim.run_action(v2_entry["id"], "上线配置")
r2 = dim.build_dimension_incremental(v2_entry["id"])
check("V2 增量构建成功", r2["ok"] and r2["config_version"] == "V2"
      and r2["scd_type"] == "SCD_TYPE_3")

# 同一客户数据集两个有效版本，V1 构建记录仍可回查
records = dim.list_build_records(dataset="customer_master")
check("两个版本构建记录并存",
      {r["config_version"] for r in records} == {"V1", "V2"})
v1_records = dim.list_build_records(dataset="customer_master", config_version="V1")
check("V1 构建记录仍可回查",
      len(v1_records) == 1 and v1_records[0]["build_id"] == v1_build_id)

# 值班员按 V1 构建记录登记历史快照任务
task, _ = snap.create_entry({
    "目标数据集": "customer_master", "快照时间点": "2026-11-23T00:00:00Z",
    "快照类型": "full", "存储路径": "", "文件大小": "", "维度配置版本": "V1",
})
task_id = task["id"]
check("快照任务登记并锁定 V1", task.get("维度配置版本") == "V1")

# 提交原任务：V1 有成功构建记录，直接完成登记，无需重选版本
res = snap.create_snapshot_with_lineage(task_id)
check("按 V1 直接完成快照登记", res["ok"] and res["config_version"] == "V1")
check("快照绑定 V1 构建记录", res["dimension_build_id"] == v1_build_id)
check("血缘同步登记", res["lineage_id"] is not None)

# 刷新详情：快照的维度版本与血缘来源一致
detail = store.find("snapshot_creator", task_id)
check("详情中版本为 V1", detail["维度配置版本"] == "V1"
      and detail["维度构建ID"] == v1_build_id)
trace = snap.trace_snapshot_lineage(task_id)
check("详情与血缘指向同一次构建", trace["consistent"] and trace["deliverable"])
check("血缘来源为 V1 构建",
      trace["lineage"][0]["config_version"] == "V1"
      and trace["lineage"][0]["dimension_build_id"] == v1_build_id)
restorable = snap.list_restorable_snapshots("customer_master")
check("历史快照列表可交付且保留 V1", any(
    s["id"] == task_id and s["config_version"] == "V1"
    and s["dimension_build_id"] == v1_build_id
    for s in restorable["snapshots"]))

# 仅生成计划的配置：V3 只 plan 不 build
v3_entry, _ = dim.create_entry({
    "配置名称": "dim_customer_v3_draft", "来源表": "customer_master",
    "维度键": "customer_id", "缓慢变化处理": "SCD_TYPE_2",
    "历史快照": "", "配置版本": "V3",
})
dim.run_action(v3_entry["id"], "上线配置")
plan = dim.generate_dimension_plan(v3_entry["id"])
check("V3 仅生成计划", plan["config_version"] == "V3")
check("计划不产生构建记录",
      len(dim.list_build_records(config_version="V3")) == 0)

task3, _ = snap.create_entry({
    "目标数据集": "customer_master", "快照时间点": "2026-11-23T01:00:00Z",
    "快照类型": "full", "存储路径": "", "文件大小": "", "维度配置版本": "V3",
})
res3 = snap.create_snapshot_with_lineage(task3["id"])
check("仅计划的配置不得结案", not res3["ok"])
check("未产生已完成快照",
      store.find("snapshot_creator", task3["id"])["status"] != "已完成")
check("未登记血缘",
      snap.trace_snapshot_lineage(task3["id"])["lineage_count"] == 0)
check("不进历史快照列表", all(
    s["id"] != task3["id"]
    for s in snap.list_restorable_snapshots("customer_master")["snapshots"]))

print("场景二：结案后血缘写入前中断的补办与幂等")

# 另一个按 V2 登记、已通过来源校验的任务，结案后血缘写入前中断
task4, _ = snap.create_entry({
    "目标数据集": "customer_master", "快照时间点": "2026-11-23T02:00:00Z",
    "快照类型": "incremental", "存储路径": "", "文件大小": "",
    "维度配置版本": "V2",
})
tid4 = task4["id"]
interrupted = snap.create_snapshot_with_lineage(tid4, fail_before_lineage=True)
check("任务写为已完成但血缘未登记", not interrupted["ok"]
      and store.find("snapshot_creator", tid4)["status"] == "已完成")
check("中断前已选定 V2 构建",
      interrupted["dimension_build_id"] == r2["build_id"])

# 刷新后：历史快照列表与血缘追溯均不可交付
check("中断任务不进历史快照列表", all(
    s["id"] != tid4
    for s in snap.list_restorable_snapshots("customer_master")["snapshots"]))
trace4 = snap.trace_snapshot_lineage(tid4)
check("血缘追溯不可交付",
      not trace4["deliverable"] and trace4["lineage_count"] == 0)

# 期间完成新版客户维度 V3 的构建
r3 = dim.build_dimension_incremental(v3_entry["id"])
check("新版 V3 构建完成", r3["ok"] and r3["config_version"] == "V3")

# 从原快照任务重试：补上的来源仍是中断前选定的 V2 构建
retry = snap.create_snapshot_with_lineage(tid4)
check("补办成功", retry["ok"] and retry["repaired"])
check("补办来源仍是中断前选定的 V2 构建",
      retry["dimension_build_id"] == r2["build_id"]
      and retry["config_version"] == "V2")
trace4b = snap.trace_snapshot_lineage(tid4)
check("补办后详情与血缘一致", trace4b["consistent"] and trace4b["deliverable"])
check("补办血缘指向 V2 构建",
      trace4b["lineage"][0]["dimension_build_id"] == r2["build_id"])
check("补办后进入历史快照列表", any(
    s["id"] == tid4 and s["config_version"] == "V2"
    for s in snap.list_restorable_snapshots("customer_master")["snapshots"]))

# 成功后再提交原任务：幂等，不多出血缘
lineage_before = len(store.rows("lineage_graph"))
again = snap.create_snapshot_with_lineage(tid4)
check("重复提交返回相同快照编号", again["ok"] and again["snapshot_id"] == tid4)
check("来源版本仍相同",
      again["config_version"] == "V2"
      and again["dimension_build_id"] == r2["build_id"])
check("没有多出血缘关系",
      len(store.rows("lineage_graph")) == lineage_before
      and snap.trace_snapshot_lineage(tid4)["lineage_count"] == 1)

# 既有已完成快照（无维度配置的数据集）血缘在册、可交付
trace_seed = snap.trace_snapshot_lineage(1)
check("既有快照血缘在册可交付",
      trace_seed["deliverable"] and trace_seed["lineage_count"] == 1)

print(f"\n全部 {checks} 项检查通过")
