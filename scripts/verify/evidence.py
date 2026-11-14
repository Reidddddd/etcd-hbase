"""Read actual test evidence; missing, skipped and contradictory results fail closed."""
import json
import subprocess
from pathlib import Path
import xml.etree.ElementTree as ET

JAVA_MODULES = ("protocol", "spi", "core", "hbase", "grpc", "admin", "server")
GO_PACKAGES = {
    "cmd/m0driver",
    "cmd/m3driver",
    "cmd/m3history",
    "cmd/m4driver",
    "cmd/native-etcd",
    "internal/client",
    "internal/compare",
    "internal/history",
    "internal/rpctypes",
    "internal/scenario",
    "internal/trace",
    "internal/wire",
}
GO_MODULE = "github.com/Reidddddd/etcd-hbase/testkit/go/"


def java_reports(root, required, modules=JAVA_MODULES, required_cases=None,
                 report_kinds=("surefire-reports", "failsafe-reports")):
    required_cases = required_cases or {}
    paths = [path for module in modules for kind in report_kinds
             for path in sorted((root / module / "target" / kind).glob("TEST-*.xml"))]
    suites = {}
    for path in paths:
        suite = ET.parse(path).getroot()
        counts = {name: int(suite.attrib[name]) for name in ("tests", "failures", "errors", "skipped")}
        name = suite.attrib["name"]
        cases = suite.findall("testcase")
        if name in suites or counts["tests"] < 1 or any(counts[key] for key in ("failures", "errors", "skipped")):
            raise ValueError("duplicate, empty, failed or skipped Java suite")
        if len(cases) != counts["tests"] or any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")):
            raise ValueError("Java testcase evidence contradicts counters")
        if not set(required_cases.get(name, ())).issubset({case.get("name") for case in cases}):
            raise ValueError("required Java testcase evidence is missing")
        suites[name] = counts
    if not set(required).issubset(suites):
        raise ValueError("required Java suite evidence is missing")
    if not set(required_cases).issubset(suites):
        raise ValueError("required Java testcase suite evidence is missing")
    return paths, suites


def go_report(path):
    if not path.is_file() or path.stat().st_size > 128 << 20:
        raise ValueError("Go test evidence is missing or oversized")
    packages, test_events = set(), {}
    with path.open() as stream:
        for line in stream:
            event = json.loads(line)
            action, package, test = event.get("Action"), event.get("Package", ""), event.get("Test")
            if action in {"skip", "fail"}:
                raise ValueError("failed or skipped Go test evidence")
            if action != "pass":
                continue
            if not package.startswith(GO_MODULE):
                raise ValueError("unknown Go module in test evidence")
            package = package.removeprefix(GO_MODULE)
            if test:
                test_events.setdefault(package, set()).add(test)
            else:
                if package in packages:
                    raise ValueError("duplicate Go package evidence")
                packages.add(package)
    if packages != GO_PACKAGES or any(not test_events.get(package) for package in packages):
        raise ValueError("missing Go packages or actual test events")
    return {"packages": sorted(packages), "passed_test_events": sum(len(items) for items in test_events.values()),
            "skipped": 0, "failed": 0}


def require_unchanged_source(root, source):
    def git(*arguments):
        return subprocess.check_output(["git", "-C", str(root), *arguments])
    if source["tracked_dirty"] or git("rev-parse", "HEAD").decode().strip() != source["checkout"]:
        raise ValueError("verification source changed or started dirty")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise ValueError("tracked source changed during verification")
    local_only = ("docs/progress/", "docs/superpowers/", "scripts/verify/__pycache__/")
    paths = git("ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")
    if any(path and not path.startswith(local_only) for path in paths):
        raise ValueError("uncommitted source exists outside local execution records")


M0_JAVA_MODULES = (*JAVA_MODULES, "testkit")
M0_REQUIRED_SUITES = {
    "io.etcdhbase.protocol.ProtocolSourceLockTest", "io.etcdhbase.protocol.ProtocolWireContractTest",
    "io.etcdhbase.protocol.ProtocolBinaryRoundTripTest", "io.etcdhbase.spi.salt.HashSaltStrategyTest",
    "io.etcdhbase.core.request.RequestScopeTest", "io.etcdhbase.hbase.identity.HBaseBackendIdentityProviderTest",
    "io.etcdhbase.hbase.probe.LocalHBaseProbeIT", "io.etcdhbase.hbase.probe.SecureHBaseAuthenticationIT",
    "io.etcdhbase.admin.InitializeTablesIT", "io.etcdhbase.admin.SaltDistributionIT",
    "io.etcdhbase.server.GoInteropIT", "io.etcdhbase.server.GeneralGoInteropIT",
    "io.etcdhbase.server.ExternalProtocolIT", "io.etcdhbase.server.RequestLifecycleTest",
    "io.etcdhbase.testkit.contract.IdentityProviderContractTest", "io.etcdhbase.testkit.contract.SaltStrategyContractTest",
}
