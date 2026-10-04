import sys
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci import ReportError, inspect_reports, source_test_classes
import ci


class ReportGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def report(self, name="lab.ExampleTest", *, skipped=False, failures=0, count=1):
        case = '<testcase name="example">' + ('<skipped/>' if skipped else '') + '</testcase>'
        (self.root / ("TEST-" + name + ".xml")).write_text(
            f'<testsuite name="{name}" tests="{count}" failures="{failures}" '
            f'errors="0" skipped="{int(skipped)}">{case}</testsuite>', encoding="utf-8")

    def test_green_report_is_accepted(self):
        self.report()
        self.assertEqual(inspect_reports(self.root, {"lab.ExampleTest"}), (1, 0))

    def test_missing_selected_class_is_rejected(self):
        self.report()
        with self.assertRaises(ReportError):
            inspect_reports(self.root, {"lab.ExampleTest", "lab.MissingTest"})

    def test_empty_report_directory_is_rejected(self):
        with self.assertRaises(ReportError):
            inspect_reports(self.root, {"lab.ExampleTest"})

    def test_linux_skip_is_rejected(self):
        self.report(skipped=True)
        with self.assertRaises(ReportError):
            inspect_reports(self.root, {"lab.ExampleTest"})

    def test_windows_only_allows_explicit_test_skips(self):
        self.report(skipped=True)
        with self.assertRaises(ReportError):
            inspect_reports(self.root, {"lab.ExampleTest"}, windows=True)

    def test_documented_windows_class_skip_is_accepted(self):
        name = "vn.huyqt.logbroker.integration.ClusterRoutingTest"
        self.report(name, skipped=True)
        self.assertEqual(inspect_reports(self.root, {name}, windows=True), (1, 1))

    def test_unprotected_cli_method_cannot_be_skipped(self):
        name = "vn.huyqt.logbroker.integration.BrokerCliSmokeTest"
        self.report(name, skipped=True)
        with self.assertRaises(ReportError):
            inspect_reports(self.root, {name}, windows=True)

    def test_failure_is_rejected(self):
        self.report(failures=1)
        with self.assertRaises(ReportError):
            inspect_reports(self.root, {"lab.ExampleTest"})

    def test_inconsistent_count_is_rejected(self):
        self.report(count=2)
        with self.assertRaises(ReportError):
            inspect_reports(self.root, {"lab.ExampleTest"})

    def test_duplicate_suite_is_rejected(self):
        self.report()
        (self.root / "TEST-duplicate.xml").write_bytes(
            (self.root / "TEST-lab.ExampleTest.xml").read_bytes())
        with self.assertRaises(ReportError):
            inspect_reports(self.root, {"lab.ExampleTest"})

    def test_source_discovery_uses_declared_package(self):
        (self.root / "ExampleTest.java").write_text(
            "package lab; class ExampleTest {}", encoding="utf-8")
        self.assertEqual(source_test_classes(self.root), {"lab.ExampleTest"})

    def test_selected_phase_cannot_reuse_old_green_report(self):
        self.report()
        reports = self.root / "target" / "surefire-reports"
        reports.mkdir(parents=True)
        (self.root / "TEST-lab.ExampleTest.xml").rename(reports / "TEST-lab.ExampleTest.xml")
        with patch.object(ci, "ROOT", self.root), patch.dict(
                os.environ, {"RUNNER_TEMP": str(self.root / "runner")}), patch.object(
                ci, "run_logged"):
            with self.assertRaises(ReportError):
                ci.phase("mvn", "selected", ["test"], {"lab.ExampleTest"}, False)
        self.assertFalse((reports / "TEST-lab.ExampleTest.xml").exists())

    def test_failed_maven_retains_reports_and_logs(self):
        def failed_run(command, log):
            log.write_text("Maven failed", encoding="utf-8")
            reports = self.root / "target" / "surefire-reports"
            reports.mkdir(parents=True)
            (reports / "TEST-failed.xml").write_text("failed evidence", encoding="utf-8")
            logs = self.root / "target" / "cluster-process-logs"
            logs.mkdir()
            (logs / "broker.log").write_text("startup failure", encoding="utf-8")
            raise RuntimeError("Maven failed")

        with patch.object(ci, "ROOT", self.root), patch.dict(
                os.environ, {"RUNNER_TEMP": str(self.root / "runner")}), patch.object(
                ci, "run_logged", side_effect=failed_run):
            with self.assertRaises(RuntimeError):
                ci.phase("mvn", "full", ["clean", "verify"], {"lab.ExampleTest"}, False)
        evidence = self.root / "runner" / "logbroker-ci-evidence" / "full"
        self.assertEqual((evidence / "maven.log").read_text(), "Maven failed")
        self.assertTrue((evidence / "surefire-reports" / "TEST-failed.xml").exists())
        self.assertTrue((evidence / "cluster-process-logs" / "broker.log").exists())


if __name__ == "__main__":
    unittest.main()
