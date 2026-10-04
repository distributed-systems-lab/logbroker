"""Run the existing Maven verification contracts and reject missing/skipped evidence."""

import argparse
import os
import platform
import re
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INTEGRATION = "vn.huyqt.logbroker.integration."
PROCESS_CLASSES = {
    INTEGRATION + name for name in (
        "ClusterRoutingTest", "ClusterControllerLossTest", "ClusterBrokerRestartTest",
        "ClusterObserverCatchupTest", "ClusterProvisioningCrashTest", "BrokerCliSmokeTest",
    )
}
CLUSTER_CLASSES = {"vn.huyqt.logbroker.broker.cluster.ClusterFaultCampaignTest"}
QUORUM_CLASSES = {
    "vn.huyqt.logbroker.controller.consensus.QuorumFaultCampaignTest",
    "vn.huyqt.logbroker.controller.consensus.LinearizabilityHistoryTest",
}
WINDOWS_SKIP_CLASSES = (PROCESS_CLASSES - {INTEGRATION + "BrokerCliSmokeTest"}) | {
    INTEGRATION + name for name in (
        "ControllerClusterTest", "ControllerCrashTest", "ControllerSnapshotCatchupTest",
    )
}
WINDOWS_SKIP_METHODS = {
    "productionCliRejectsUnformattedRootAndDuplicateBrokerIdentity",
    "productionExampleRoutesSixPartitionsFromOneBootstrap",
}


class ReportError(ValueError):
    pass


def source_test_classes(source):
    names = set()
    for path in source.rglob("*Test.java"):
        package = re.search(r"^package\s+([\w.]+)\s*;", path.read_text(encoding="utf-8"), re.M)
        if package is None:
            raise ReportError(f"Missing package declaration: {path}")
        names.add(package[1] + "." + path.stem)
    if not names:
        raise ReportError("No test source classes discovered")
    return names


def inspect_reports(directory, expected, *, windows=False):
    """Count only selected suites; every expected class must have fresh nonempty evidence."""
    found = set()
    tests = skipped = 0
    for path in sorted(directory.glob("TEST-*.xml")):
        try:
            suite = ET.parse(path).getroot()
            name = suite.attrib["name"]
            if name not in expected:
                continue
            if suite.tag != "testsuite" or name in found:
                raise ReportError(f"Invalid or duplicate suite: {path}")
            cases = suite.findall("testcase")
            count = int(suite.attrib["tests"])
            skips = sum(case.find("skipped") is not None for case in cases)
            if count <= 0 or count != len(cases) or skips != int(suite.attrib["skipped"]):
                raise ReportError(f"Invalid test counts: {path}")
            if (int(suite.attrib["failures"]) != 0 or int(suite.attrib["errors"]) != 0
                    or any(case.find("failure") is not None or case.find("error") is not None
                           for case in cases)):
                raise ReportError(f"Failed suite: {path}")
            for case in cases:
                if case.find("skipped") is None:
                    continue
                permitted = name in WINDOWS_SKIP_CLASSES or (
                    name == INTEGRATION + "BrokerCliSmokeTest"
                    and case.attrib.get("name") in WINDOWS_SKIP_METHODS)
                if not windows or not permitted:
                    raise ReportError(f"Unexpected skip: {name}.{case.attrib.get('name')}")
            found.add(name)
            tests += count
            skipped += skips
        except (ET.ParseError, KeyError, ValueError) as error:
            raise ReportError(f"Invalid report {path}: {error}") from error
    if not expected or found != expected:
        raise ReportError(f"Missing test suites: {sorted(expected - found)}")
    return tests, skipped


def preflight(windows):
    if (platform.system() == "Windows") != windows:
        raise RuntimeError("CI profile does not match the operating system")
    if not windows and platform.system() != "Linux":
        raise RuntimeError("Strict verification requires Linux")
    maven = shutil.which("mvn")
    if not maven:
        raise RuntimeError("Maven must be on PATH")
    version = subprocess.check_output([maven, "--version"], text=True, stderr=subprocess.STDOUT)
    print(version, flush=True)
    if not re.search(r"Apache Maven 3\.9\.\d+", version):
        raise RuntimeError("CI requires Maven 3.9.x")
    if not re.search(r"Java version: 21(?:[.,\s]|$)", version):
        raise RuntimeError("Maven must run on Java 21")
    if not windows:
        # Both JUnit temporary process roots and the source/build root must be ext4.
        for path in (ROOT, Path(tempfile.gettempdir())):
            fs = subprocess.check_output(
                ["findmnt", "-T", str(path), "-n", "-o", "FSTYPE"], text=True).strip()
            if fs != "ext4":
                raise RuntimeError(f"Strict verification requires ext4: {path} uses {fs}")
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            print(f"ext4 and directory-force preflight passed: {path}", flush=True)
        # Real controller/broker formatting also probes DurableFiles in the process tests.
    return maven


def run_logged(command, log):
    with log.open("w", encoding="utf-8") as output:
        with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True,
                              encoding="utf-8", errors="replace") as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                output.write(line)
            result = process.wait()
    if result:
        raise RuntimeError(f"Command failed with exit {result}; see {log}")


def phase(maven, name, arguments, expected, windows):
    # Maven clean deletes target; retain evidence outside the checkout's build directory.
    evidence_root = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())) / "logbroker-ci-evidence"
    if evidence_root.resolve().is_relative_to((ROOT / "target").resolve()):
        raise RuntimeError("Evidence must live outside Maven's target directory")
    evidence = evidence_root / name
    evidence.mkdir(parents=True, exist_ok=False)
    reports = ROOT / "target" / "surefire-reports"
    if reports.is_symlink():
        raise RuntimeError("Surefire reports must not be a symlink")
    # Selected runs reuse compiled classes, but must never reuse earlier successful reports.
    for path in reports.glob("*"):
        if path.is_file():
            path.unlink()
    try:
        run_logged([maven, "-B", "-ntp", "-DargLine=-Xms32m -Xmx512m", *arguments],
                   evidence / "maven.log")
    finally:
        if reports.is_dir():
            shutil.copytree(reports, evidence / "surefire-reports")
        for name in ("controller-process-logs", "cluster-process-logs"):
            logs = ROOT / "target" / name
            if logs.is_dir():
                shutil.copytree(logs, evidence / name)
    tests, skipped = inspect_reports(evidence / "surefire-reports", expected, windows=windows)
    summary = f"{evidence.name}: {tests} tests, {skipped} skips, {len(expected)} expected classes"
    print(summary, flush=True)
    (evidence / "summary.txt").write_text(summary + "\n", encoding="utf-8")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
            output.write(f"- {summary}\n")


def selectors(classes):
    return "-Dtest=" + ",".join(sorted(name.rsplit(".", 1)[1] for name in classes))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=("linux", "windows", "extended"))
    args = parser.parse_args()
    windows = args.profile == "windows"
    maven = preflight(windows)
    os.environ["MAVEN_OPTS"] = "-Xms32m -Xmx512m"
    phase(maven, "full", ["clean", "verify", "dependency:copy-dependencies"],
          source_test_classes(ROOT / "src" / "test" / "java"), windows)
    if args.profile == "extended":
        phase(maven, "cluster100", [selectors(CLUSTER_CLASSES), "-Dcluster.seedCount=100", "test"],
              CLUSTER_CLASSES, False)
        phase(maven, "quorum100", [selectors(QUORUM_CLASSES), "-Dquorum.seedCount=100", "test"],
              QUORUM_CLASSES, False)
        phase(maven, "processes", [selectors(PROCESS_CLASSES), "test"], PROCESS_CLASSES, False)
        # Kept beneath target so clean starts fresh and artifacts have a known location.
        demo = ROOT / "target" / "ci-demo"
        demo_log = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())) / "logbroker-ci-evidence" / "demo.log"
        run_logged(["bash", "scripts/cluster-demo.sh", str(demo)],
                   demo_log)
        if "SUCCESS records=6 brokers=3" not in demo_log.read_text(encoding="utf-8"):
            raise RuntimeError("Demo did not establish six records on three brokers")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        raise SystemExit(f"CI verification failed: {error}")
