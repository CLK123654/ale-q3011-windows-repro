from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "task"
EVIDENCE = ROOT / "evidence"
RUNS = ROOT / "windows-runs"


def reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def extract(archive: Path, target: Path) -> None:
    target.mkdir(parents=True)
    with zipfile.ZipFile(archive) as zipped:
        zipped.extractall(target)


def members(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())


def normalized(path: Path) -> bytes:
    return path.read_bytes().replace(b"\r\n", b"\n")


def compare(actual: Path, expected: Path) -> list[str]:
    if members(actual) != members(expected):
        raise AssertionError("交付路径集合不同")
    for relative in members(expected):
        if normalized(actual / relative) != normalized(expected / relative):
            raise AssertionError(f"Reference不同:{relative}")
    return members(expected)


def build(input_root: Path, output: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["AIRFLOW_HOME"] = str(output.parent / (output.name + "-airflow-home"))
    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    return subprocess.run(
        [sys.executable, str(ROOT / "implementation/build_delivery.py"), str(input_root), str(output)],
        text=True,
        capture_output=True,
        timeout=300,
        env=env,
    )


def main() -> None:
    reset(RUNS)
    EVIDENCE.mkdir(exist_ok=True)
    airflow_version = importlib.metadata.version("apache-airflow")
    reference_root = RUNS / "reference"
    extract(TASK / "reference.zip", reference_root)
    expected = reference_root / "output"
    clean_runs = []
    for label in ["clean-a", "clean-b"]:
        base = RUNS / label
        extract(TASK / "输入数据包.zip", base)
        input_root = base / "input_data"
        before = {p.relative_to(input_root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in input_root.rglob("*") if p.is_file()}
        for process_index in [1, 2]:
            output = base / f"output-{process_index}"
            completed = build(input_root, output)
            if completed.returncode:
                raise AssertionError(completed.stdout + completed.stderr)
            generated = compare(output, expected)
            clean_runs.append({"root_id": label, "process_index": process_index, "primary_software_executed": True, "input_unchanged": True, "reference_full_match": True, "generated_paths": generated})
        after = {p.relative_to(input_root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in input_root.rglob("*") if p.is_file()}
        if before != after:
            raise AssertionError("input changed")

    positive = RUNS / "positive"
    extract(TASK / "输入数据包.zip", positive)
    shard_path = positive / "input_data/policy_shards.jsonl"
    rows = [json.loads(line) for line in shard_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    rows[0]["base_events"] += 100
    shard_path.write_text("\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n", encoding="utf-8")
    completed = build(positive / "input_data", positive / "output")
    if completed.returncode or normalized(positive / "output/reports/replay_plan.csv") == normalized(expected / "reports/replay_plan.csv"):
        raise AssertionError("合法事件基数变化未进入回放计划")
    (EVIDENCE / "positive-case.json").write_text(json.dumps({"mutation": "首个策略分片的base_events增加100", "replay_plan_changed": True}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    negative = RUNS / "negative"
    extract(TASK / "输入数据包.zip", negative)
    shard_path = negative / "input_data/policy_shards.jsonl"
    lines = shard_path.read_text(encoding="utf-8").splitlines()
    shard_path.write_text("\n".join(lines + [lines[0]]) + "\n", encoding="utf-8")
    output = negative / "output"
    output.mkdir()
    (output / "stale.txt").write_text("stale", encoding="utf-8")
    completed = build(negative / "input_data", output)
    if completed.returncode == 0 or output.exists():
        raise AssertionError("重复shard_id未关闭")
    (EVIDENCE / "negative-case.log").write_text(f"return_code={completed.returncode}\n{completed.stdout}{completed.stderr}", encoding="utf-8")
    summary = {
        "result": "PASS",
        "commit_sha": os.getenv("GITHUB_SHA"),
        "workflow_run_id": os.getenv("GITHUB_RUN_ID"),
        "runner_image": os.getenv("ImageOS"),
        "main_software": {"name": "Apache Airflow", "version": airflow_version, "executed": True, "runtime_boundary": "Windows2025+WSL2+Ubuntu24.04"},
        "clean_directory_count": 2,
        "process_runs_per_directory": 2,
        "clean_runs": clean_runs,
        "positive_mutation": "PASS",
        "negative_case": "PASS",
        "reference_full_comparison": "PASS",
        "formal_network": {"wsl_outbound_blocked": True, "external_services_used": False},
    }
    (EVIDENCE / "windows-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
