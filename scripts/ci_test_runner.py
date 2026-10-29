#!/usr/bin/env python3
"""Build, package, and run the exact test harnesses used by CI."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from typing import Any


PACKAGE_NAME = "ipfs-s3-gateway"
MANIFEST_PATH = Path("target/ci/ci-test-harnesses.json")
ARCHIVE_PATH = Path("target/ci-test-harnesses.tar.gz")

TEST_TARGETS = (
    "integration",
    "http_admission",
    "object_read_snapshot",
    "bucket_config_owner_atomicity",
    "schema_baseline",
    "schema_baseline_semantics",
)

HARNESS_SPECS = {
    "lib": ("lib", "ipfs_s3_gateway", True),
    "bin": ("bin", PACKAGE_NAME, True),
    **{name: ("test", name, True) for name in TEST_TARGETS},
}
GATEWAY_SPEC = ("bin", PACKAGE_NAME, False)

SHARDS = (
    "core-legacy-remainder",
    "worker-providers",
    "store-pinning-control",
    "management",
)


class RunnerError(RuntimeError):
    pass


def classify_lib_test(name: str) -> str:
    if name.startswith("store::pinning::management::"):
        return "management"
    if name.startswith("store::pinning::legacy_migration::"):
        return "core-legacy-remainder"
    if name.startswith("store::pinning::"):
        return "store-pinning-control"
    if name.startswith("pinning::"):
        return "worker-providers"
    return "core-legacy-remainder"


def partition_lib_tests(names: list[str]) -> dict[str, list[str]]:
    partitions = {shard: [] for shard in SHARDS}
    for name in names:
        partitions[classify_lib_test(name)].append(name)
    return partitions


def parse_test_list(output: str) -> list[str]:
    names = []
    for line in output.splitlines():
        name, separator, kind = line.rpartition(": ")
        if separator and kind == "test":
            names.append(name)
    if len(names) != len(set(names)):
        raise RunnerError("the library harness listed a test name more than once")
    return names


def _root_package_id(root: Path) -> str:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise RunnerError(f"cargo metadata failed with exit code {result.returncode}")

    metadata = json.loads(result.stdout)
    manifest_path = (root / "Cargo.toml").resolve()
    packages = [
        package
        for package in metadata["packages"]
        if Path(package["manifest_path"]).resolve() == manifest_path
    ]
    if len(packages) != 1 or packages[0]["name"] != PACKAGE_NAME:
        raise RunnerError("cargo metadata did not identify the root ipfs-s3-gateway package")
    return str(packages[0]["id"])


def artifact_key(message: dict[str, Any], package_id: str) -> str | None:
    if message.get("reason") != "compiler-artifact":
        return None
    if message.get("package_id") != package_id or not message.get("executable"):
        return None

    target = message.get("target", {})
    kinds = target.get("kind", [])
    if len(kinds) != 1:
        return None
    spec = (kinds[0], target.get("name"), message.get("profile", {}).get("test"))
    if spec == GATEWAY_SPEC:
        return "gateway"
    for key, expected in HARNESS_SPECS.items():
        if spec == expected:
            return key
    return None


def _relative_artifact(root: Path, raw_path: str) -> Path:
    path = Path(raw_path).resolve()
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise RunnerError(f"Cargo artifact is outside the repository: {path}") from error
    if not path.is_file():
        raise RunnerError(f"Cargo reported a missing executable: {path}")
    return relative


def _write_archive(root: Path, artifacts: dict[str, Path]) -> None:
    manifest_path = root / MANIFEST_PATH
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "format": 1,
        "build_root": str(root),
        "harnesses": {
            key: artifacts[key].as_posix() for key in HARNESS_SPECS
        },
        "runtime_files": {"gateway": artifacts["gateway"].as_posix()},
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    archive_path = root / ARCHIVE_PATH
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    archived_paths = {MANIFEST_PATH, *artifacts.values()}
    with tarfile.open(
        archive_path, mode="w:gz", compresslevel=1, format=tarfile.PAX_FORMAT
    ) as archive:
        for relative in sorted(archived_paths, key=lambda path: path.as_posix()):
            archive.add(root / relative, arcname=relative.as_posix(), recursive=False)

    print(f"Packaged {len(artifacts)} executables in {ARCHIVE_PATH.as_posix()}")


def build_harnesses(root: Path) -> int:
    package_id = _root_package_id(root)
    command = [
        "cargo",
        "test",
        "--locked",
        "--no-run",
        "--lib",
        "--bin",
        PACKAGE_NAME,
    ]
    for target in TEST_TARGETS:
        command.extend(("--test", target))
    command.append("--message-format=json-render-diagnostics")

    process = subprocess.Popen(
        command,
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
    )
    if process.stdout is None:
        raise RunnerError("failed to capture Cargo JSON output")

    artifacts: dict[str, Path] = {}
    for raw_line in process.stdout:
        try:
            message = json.loads(raw_line)
        except json.JSONDecodeError:
            sys.stdout.write(raw_line)
            continue

        if message.get("reason") == "compiler-message":
            rendered = message.get("message", {}).get("rendered")
            if rendered:
                sys.stderr.write(rendered)
            continue

        key = artifact_key(message, package_id)
        if key is None:
            continue
        relative = _relative_artifact(root, str(message["executable"]))
        previous = artifacts.get(key)
        if previous is not None and previous != relative:
            process.kill()
            process.wait()
            raise RunnerError(f"Cargo reported multiple executables for {key}")
        artifacts[key] = relative
        print(f"Recorded {key}: {relative.as_posix()}")

    return_code = process.wait()
    if return_code != 0:
        return return_code

    expected = {*HARNESS_SPECS, "gateway"}
    missing = sorted(expected - artifacts.keys())
    if missing:
        raise RunnerError(f"Cargo did not report required executables: {', '.join(missing)}")

    _write_archive(root, artifacts)
    return 0


def _load_manifest(root: Path) -> dict[str, Path]:
    manifest_path = root / MANIFEST_PATH
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RunnerError(f"cannot read {MANIFEST_PATH.as_posix()}: {error}") from error

    if manifest.get("format") != 1:
        raise RunnerError("unsupported CI harness manifest format")
    if Path(str(manifest.get("build_root", ""))).resolve() != root:
        raise RunnerError(
            "test harnesses must run from the same absolute checkout path used to build them"
        )

    entries = {**manifest.get("harnesses", {}), **manifest.get("runtime_files", {})}
    expected = {*HARNESS_SPECS, "gateway"}
    if set(entries) != expected:
        raise RunnerError("CI harness manifest has an unexpected executable set")

    resolved = {}
    for key, raw_relative in entries.items():
        relative = Path(str(raw_relative))
        if relative.is_absolute():
            raise RunnerError(f"manifest path for {key} is absolute")
        path = (root / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError as error:
            raise RunnerError(f"manifest path for {key} leaves the repository") from error
        if not path.is_file():
            raise RunnerError(f"manifest executable for {key} is missing: {relative}")
        resolved[key] = path
    return resolved


def _list_lib_tests(executable: Path, root: Path) -> tuple[int, list[str]]:
    result = subprocess.run(
        [str(executable), "--list", "--format", "terse"],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.stderr:
        sys.stderr.write(result.stderr)
    if result.returncode != 0:
        return result.returncode, []
    names = parse_test_list(result.stdout)
    if not names:
        raise RunnerError("the library harness listed no tests")
    return 0, names


def _check_linux_argument_budget(command: list[str]) -> int:
    argument_bytes = sum(len(os.fsencode(argument)) + 1 for argument in command)
    if os.name != "nt" and hasattr(os, "sysconf"):
        argument_limit = int(os.sysconf("SC_ARG_MAX"))
        environment_bytes = sum(
            len(os.fsencode(key)) + len(os.fsencode(value)) + 2
            for key, value in os.environ.items()
        )
        if argument_bytes + environment_bytes + 65536 >= argument_limit:
            raise RunnerError(
                f"library shard command needs {argument_bytes} argument bytes, "
                f"which exceeds the available Linux exec budget"
            )
    return argument_bytes


def run_lib_shard(executable: Path, shard: str, root: Path) -> int:
    list_return_code, names = _list_lib_tests(executable, root)
    if list_return_code != 0:
        return list_return_code

    partitions = partition_lib_tests(names)
    if sum(len(group) for group in partitions.values()) != len(names):
        raise RunnerError("library test partition did not preserve every listed test")
    selected = partitions[shard]
    print(
        "Library shard sizes: "
        + ", ".join(f"{name}={len(partitions[name])}" for name in SHARDS)
    )
    if not selected:
        raise RunnerError(
            f"library shard {shard} selected no tests; refusing an unfiltered harness run"
        )

    command = [str(executable), "--exact", *selected]
    argument_bytes = _check_linux_argument_budget(command)
    print(f"Running {len(selected)} tests in {shard} ({argument_bytes} argument bytes)")
    return subprocess.run(command, cwd=root).returncode


def run_harnesses(executables: dict[str, Path], names: list[str], root: Path) -> int:
    for name in names:
        print(f"Running {name}: {executables[name].relative_to(root)}", flush=True)
        return_code = subprocess.run([str(executables[name])], cwd=root).returncode
        if return_code != 0:
            return return_code
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("build", help="compile and package the CI harness set")
    shard_parser = subparsers.add_parser("run-lib-shard", help="run one library shard")
    shard_parser.add_argument("shard", choices=SHARDS)
    harness_parser = subparsers.add_parser("run-harnesses", help="run named harnesses")
    harness_parser.add_argument("names", nargs="+", choices=tuple(HARNESS_SPECS))
    args = parser.parse_args()

    root = Path.cwd().resolve()
    try:
        if args.command == "build":
            return build_harnesses(root)
        executables = _load_manifest(root)
        if args.command == "run-lib-shard":
            return run_lib_shard(executables["lib"], args.shard, root)
        return run_harnesses(executables, args.names, root)
    except RunnerError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
