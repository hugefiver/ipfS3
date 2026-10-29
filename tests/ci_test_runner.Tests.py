#!/usr/bin/env python3

from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = spec_from_file_location("ci_test_runner", ROOT / "scripts" / "ci_test_runner.py")
assert SPEC is not None and SPEC.loader is not None
RUNNER = module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class CiTestRunnerTests(unittest.TestCase):
    def test_partitions_are_exhaustive_and_mutually_exclusive(self) -> None:
        names = [
            "config::tests::new_top_level_case",
            "store::pinning::legacy_migration::live_legacy_points_jobs_and_poll_share_remaining_receipt_work",
            "pinning::worker::tests::polls",
            "store::pinning::publication::tests::publishes",
            "store::pinning::management::actions::execution::review_tests::executed_submit_rejects_birth_after_read_attestation_expiry",
        ]

        partitions = RUNNER.partition_lib_tests(names)

        selected = [test for shard in RUNNER.SHARDS for test in partitions[shard]]
        self.assertCountEqual(selected, names)
        self.assertEqual(len(selected), len(set(selected)))
        self.assertIn(names[0], partitions["core-legacy-remainder"])
        self.assertIn(names[1], partitions["core-legacy-remainder"])
        self.assertIn(names[2], partitions["worker-providers"])
        self.assertIn(names[3], partitions["store-pinning-control"])
        self.assertIn(names[4], partitions["management"])

    def test_unknown_module_goes_to_remainder(self) -> None:
        self.assertEqual(
            RUNNER.classify_lib_test("future_module::tests::case"),
            "core-legacy-remainder",
        )

    def test_cargo_artifact_identity_uses_package_target_and_test_profile(self) -> None:
        message = {
            "reason": "compiler-artifact",
            "package_id": "root-package",
            "target": {"kind": ["lib"], "name": "ipfs_s3_gateway"},
            "profile": {"test": True},
            "executable": "/repo/target/ci/debug/deps/lib-harness",
        }

        self.assertEqual(RUNNER.artifact_key(message, "root-package"), "lib")
        self.assertIsNone(RUNNER.artifact_key(message, "dependency-package"))
        message["profile"]["test"] = False
        self.assertIsNone(RUNNER.artifact_key(message, "root-package"))

    def test_empty_shard_does_not_run_the_harness_unfiltered(self) -> None:
        listed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="pinning::worker::tests::only: test\n", stderr=""
        )
        with mock.patch.object(RUNNER.subprocess, "run", return_value=listed) as run:
            with self.assertRaises(RUNNER.RunnerError):
                RUNNER.run_lib_shard(Path("fake-harness"), "management", ROOT)

        self.assertEqual(run.call_count, 1)
        self.assertIn("--list", run.call_args.args[0])

    def test_harness_failure_exit_code_is_propagated(self) -> None:
        listed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="pinning::worker::tests::only: test\n", stderr=""
        )
        failed = subprocess.CompletedProcess(args=[], returncode=23, stdout="", stderr="")
        with mock.patch.object(RUNNER.subprocess, "run", side_effect=[listed, failed]) as run:
            return_code = RUNNER.run_lib_shard(
                Path("fake-harness"), "worker-providers", ROOT
            )

        self.assertEqual(return_code, 23)
        command = run.call_args_list[1].args[0]
        self.assertIn("--exact", command)
        self.assertIn("pinning::worker::tests::only", command)

    def test_list_parser_rejects_duplicate_names(self) -> None:
        with self.assertRaises(RUNNER.RunnerError):
            RUNNER.parse_test_list("same: test\nsame: test\n")

    def test_gzip_archive_extracts_at_relative_paths_and_preserves_executable_mode(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            artifacts = {}
            for index, key in enumerate([*RUNNER.HARNESS_SPECS, "gateway"]):
                relative = (
                    Path("target/ci/debug/ipfs-s3-gateway")
                    if key == "gateway"
                    else Path("target/ci/debug/deps") / f"{index}-{key}"
                )
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(key, encoding="utf-8")
                if os.name != "nt":
                    path.chmod(0o755)
                artifacts[key] = relative

            RUNNER._write_archive(root, artifacts)

            archive_path = root / RUNNER.ARCHIVE_PATH
            self.assertEqual(archive_path.read_bytes()[:2], b"\x1f\x8b")
            expected_members = {
                RUNNER.MANIFEST_PATH.as_posix(),
                *(path.as_posix() for path in artifacts.values()),
            }
            with tarfile.open(archive_path, mode="r:gz") as archive:
                self.assertEqual(set(archive.getnames()), expected_members)
                if os.name != "nt":
                    self.assertNotEqual(
                        archive.getmember(artifacts["lib"].as_posix()).mode & 0o111,
                        0,
                    )

            extract_root = root / "extracted"
            extract_root.mkdir()
            subprocess.run(
                ["tar", "-xzf", str(archive_path), "-C", str(extract_root)],
                check=True,
            )
            self.assertEqual(
                (extract_root / artifacts["lib"]).read_text(encoding="utf-8"),
                "lib",
            )
            manifest = json.loads(
                (extract_root / RUNNER.MANIFEST_PATH).read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["build_root"], str(root))
            self.assertEqual(
                manifest["runtime_files"]["gateway"],
                artifacts["gateway"].as_posix(),
            )
            if os.name != "nt":
                self.assertTrue(os.access(extract_root / artifacts["lib"], os.X_OK))


if __name__ == "__main__":
    unittest.main()
