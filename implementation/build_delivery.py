from __future__ import annotations

import csv
import json
import os
import shutil
import sys
from pathlib import Path

import yaml
from airflow.models import DagBag
from airflow.models.mappedoperator import MappedOperator
from airflow.serialization.serialized_objects import SerializedDAG


ROOT = Path(__file__).resolve().parents[1]
INPUT = Path(sys.argv[1]).resolve()
OUTPUT = Path(sys.argv[2]).resolve()
SOURCE_DAG = ROOT / "implementation" / "content_moderation_replay.py"


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    required = {
        "README.md",
        "orchestration_contract.yaml",
        "policy_shards.jsonl",
        "replay_windows.csv",
        "release_request.json",
        "current_dag/content_moderation_replay.py",
    }
    actual = {p.relative_to(INPUT).as_posix() for p in INPUT.rglob("*") if p.is_file()}
    if actual != required:
        raise ValueError("输入成员与交付约定不一致")
    contract = yaml.safe_load((INPUT / "orchestration_contract.yaml").read_text(encoding="utf-8"))
    request = json.loads((INPUT / "release_request.json").read_text(encoding="utf-8"))
    sys.path.insert(0, str(ROOT / "implementation"))
    os.environ["ALE_INPUT_ROOT"] = str(INPUT)
    from content_moderation_replay import build_payloads

    payloads = build_payloads(INPUT)
    tmp = OUTPUT.with_name(OUTPUT.name + ".building")
    if tmp.exists():
        shutil.rmtree(tmp)
    (tmp / "dags").mkdir(parents=True)
    shutil.copy2(SOURCE_DAG, tmp / "dags" / "content_moderation_replay.py")

    bag = DagBag(dag_folder=str(tmp / "dags"), include_examples=False, safe_mode=False)
    if bag.import_errors:
        raise RuntimeError(json.dumps(bag.import_errors, ensure_ascii=False))
    dag = bag.dags.get(contract["dag_id"])
    if dag is None:
        raise RuntimeError("候选DAG未导入")
    serialized = SerializedDAG.to_dict(dag)

    write_csv(
        tmp / "reports" / "replay_plan.csv",
        payloads,
        ["map_index", "window_id", "logical_date", "shard_id", "queue", "pool", "planned_events", "dataset_uri", "weight_rule"],
    )
    grouped: dict[tuple[str, str], dict] = {}
    for row in payloads:
        key = (row["pool"], row["queue"])
        item = grouped.setdefault(key, {"pool": row["pool"], "queue": row["queue"], "shard_ids": set(), "window_ids": set(), "planned_events": 0})
        item["shard_ids"].add(row["shard_id"])
        item["window_ids"].add(row["window_id"])
        item["planned_events"] += row["planned_events"]
    pool_rows = [
        {
            "pool": value["pool"],
            "queue": value["queue"],
            "shard_ids": "|".join(sorted(value["shard_ids"])),
            "window_ids": "|".join(sorted(value["window_ids"])),
            "planned_events": value["planned_events"],
        }
        for value in sorted(grouped.values(), key=lambda item: (item["pool"], item["queue"]))
    ]
    write_csv(tmp / "reports" / "pool_assignment.csv", pool_rows, ["pool", "queue", "shard_ids", "window_ids", "planned_events"])
    lineage = [
        {"relation": "schedule_inlet", "producer": contract["schedule_dataset"], "consumer": contract["dag_id"]},
        {"relation": "task_outlet", "producer": "collect_shard_metrics", "consumer": contract["metrics_dataset"]},
        {"relation": "dag_outlet", "producer": "publish_replay_dataset", "consumer": contract["validated_dataset"]},
    ]
    write_csv(tmp / "reports" / "dataset_lineage.csv", lineage, ["relation", "producer", "consumer"])

    edges = sorted({(up.task_id, down.task_id) for up in dag.tasks for down in up.downstream_list})
    structure = {
        "dag_id": dag.dag_id,
        "schedule_dataset": contract["schedule_dataset"],
        "catchup": dag.catchup,
        "max_active_runs": dag.max_active_runs,
        "start_date": dag.start_date.isoformat(),
        "task_ids": dag.task_ids,
        "mapped_task_ids": [task.task_id for task in dag.tasks if isinstance(task, MappedOperator)],
        "task_pools": {task.task_id: task.pool for task in dag.tasks},
        "setup_task_ids": [task.task_id for task in dag.tasks if task.is_setup],
        "teardown_task_ids": [task.task_id for task in dag.tasks if task.is_teardown],
        "edges": [{"upstream": upstream, "downstream": downstream} for upstream, downstream in edges],
        "serialized_dag": bool(serialized),
    }
    (tmp / "reports" / "dag_structure.json").write_text(json.dumps(structure, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary = {
        **request,
        "release_owner": contract["release_owner"],
        "runtime_owner": contract["runtime_owner"],
        "dag_id": contract["dag_id"],
        "candidate_dag_path": "output/dags/content_moderation_replay.py",
        "current_dag_path": "input_data/current_dag/content_moderation_replay.py",
    }
    (tmp / "release-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (tmp / "README.md").write_text(
        "# 内容安全回放发布材料\n\n"
        "dags目录保存候选AirflowDAG。reports目录保存回放计划、资源池分配、Dataset血缘和DAG结构。"
        "release-summary.json供内容安全值班人安排维护窗切换、观察和退回。\n",
        encoding="utf-8",
    )
    if OUTPUT.exists():
        shutil.rmtree(OUTPUT)
    tmp.rename(OUTPUT)


if __name__ == "__main__":
    main()
