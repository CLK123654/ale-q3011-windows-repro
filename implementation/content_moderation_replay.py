from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import yaml
from airflow.datasets import Dataset
from airflow.decorators import dag, task


def read_business_inputs(input_root: Path) -> tuple[dict, list[dict], list[dict], dict]:
    contract = yaml.safe_load((input_root / "orchestration_contract.yaml").read_text(encoding="utf-8"))
    request = json.loads((input_root / "release_request.json").read_text(encoding="utf-8"))

    with (input_root / "replay_windows.csv").open(encoding="utf-8", newline="") as handle:
        windows = list(csv.DictReader(handle))
    shards = [
        json.loads(line)
        for line in (input_root / "policy_shards.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not windows or not shards:
        raise ValueError("回放窗口和策略分片不能为空")
    if len({row["window_id"] for row in windows}) != len(windows):
        raise ValueError("window_id必须唯一")
    if len({row["shard_id"] for row in shards}) != len(shards):
        raise ValueError("shard_id必须唯一")
    allowed_pools = {contract["cpu_pool"], contract["gpu_pool"]}
    if any(row["pool"] not in allowed_pools for row in shards):
        raise ValueError("策略分片的pool不在编排合同中")
    return contract, windows, shards, request


def build_payloads(input_root: Path) -> list[dict]:
    _contract, windows, shards, _request = read_business_inputs(input_root)
    payloads: list[dict] = []
    for window in windows:
        logical_date = datetime.fromisoformat(window["logical_date"])
        if logical_date.tzinfo is None or logical_date.utcoffset() != timezone.utc.utcoffset(logical_date):
            raise ValueError("logical_date必须是UTC时间")
        multiplier = Decimal(window["multiplier"])
        if multiplier <= 0:
            raise ValueError("multiplier必须大于零")
        for shard in shards:
            payloads.append(
                {
                    "map_index": len(payloads),
                    "window_id": window["window_id"],
                    "logical_date": window["logical_date"],
                    "shard_id": shard["shard_id"],
                    "queue": shard["queue"],
                    "pool": shard["pool"],
                    "planned_events": int((Decimal(shard["base_events"]) * multiplier).quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
                    "dataset_uri": window["event_dataset_uri"],
                    "weight_rule": shard["weight_rule"],
                }
            )
    return payloads


INPUT_ROOT = Path(os.environ.get("ALE_INPUT_ROOT", "/opt/content-safety/replay-input"))
CONTRACT = yaml.safe_load((INPUT_ROOT / "orchestration_contract.yaml").read_text(encoding="utf-8")) if (INPUT_ROOT / "orchestration_contract.yaml").exists() else {
    "dag_id": "content_moderation_replay",
    "schedule_dataset": "s3://trust-safety/moderation/events_ready",
    "metrics_dataset": "s3://trust-safety/moderation/replay_metrics",
    "validated_dataset": "s3://trust-safety/moderation/replay_validated",
    "max_active_runs": 1,
    "cpu_pool": "content_cpu",
    "gpu_pool": "gpu_review",
}


@dag(
    dag_id=CONTRACT["dag_id"],
    schedule=[Dataset(CONTRACT["schedule_dataset"])],
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=int(CONTRACT["max_active_runs"]),
    tags=["content-safety", "replay"],
)
def content_moderation_replay():
    @task
    def create_workspace() -> str:
        return "/var/tmp/content-moderation-replay"

    @task
    def load_replay_plan(workspace: str) -> list[dict]:
        _ = workspace
        return build_payloads(INPUT_ROOT)

    @task
    def select_cpu_payloads(payloads: list[dict]) -> list[dict]:
        return [row for row in payloads if row["pool"] == CONTRACT["cpu_pool"]]

    @task
    def select_gpu_payloads(payloads: list[dict]) -> list[dict]:
        return [row for row in payloads if row["pool"] == CONTRACT["gpu_pool"]]

    @task(pool=CONTRACT["cpu_pool"])
    def process_content_cpu_shard(payload: dict, workspace: str) -> dict:
        return {**payload, "workspace": workspace}

    @task(pool=CONTRACT["gpu_pool"])
    def process_gpu_review_shard(payload: dict, workspace: str) -> dict:
        return {**payload, "workspace": workspace}

    @task(outlets=[Dataset(CONTRACT["metrics_dataset"])])
    def collect_shard_metrics(cpu_results: list[dict], gpu_results: list[dict]) -> dict:
        rows = cpu_results + gpu_results
        return {"mapped_payloads": len(rows), "planned_events": sum(row["planned_events"] for row in rows)}

    @task(outlets=[Dataset(CONTRACT["validated_dataset"])])
    def publish_replay_dataset(metrics: dict) -> dict:
        if metrics["mapped_payloads"] < 1:
            raise ValueError("没有可发布的回放载荷")
        return metrics

    @task
    def cleanup_workspace(workspace: str) -> str:
        return workspace

    workspace = create_workspace()
    workspace.as_setup()
    payloads = load_replay_plan(workspace)
    cpu_results = process_content_cpu_shard.partial(workspace=workspace).expand(payload=select_cpu_payloads(payloads))
    gpu_results = process_gpu_review_shard.partial(workspace=workspace).expand(payload=select_gpu_payloads(payloads))
    metrics = collect_shard_metrics(cpu_results, gpu_results)
    published = publish_replay_dataset(metrics)
    cleanup = cleanup_workspace(workspace)
    cleanup.as_teardown(setups=workspace)
    published >> cleanup


content_moderation_replay()
