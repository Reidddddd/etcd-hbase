#!/usr/bin/env python3
"""Check public dependency settings, immutable CI actions, runtime pin and actual Go isolation."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
GO_POLICY = {"GOENV": "", "GOAUTH": "off", "GOPROXY": "https://proxy.golang.org", "GOSUMDB": "sum.golang.org",
             "GOPRIVATE": "", "GONOPROXY": "", "GONOSUMDB": "", "GOINSECURE": "", "GOVCS": "*:off",
             "GOTOOLCHAIN": "local", "GOWORK": "off", "GOFLAGS": "-mod=readonly"}
MAVEN_PREPARATION = ".github/workflows/maven-dependencies.yml"
MAVEN_CACHE_INPUTS = (
    "**/pom.xml", ".mvn/maven.config", ".mvn/wrapper/maven-wrapper.properties",
    "build/dependency-lock.json", "build/maven-settings-public.xml", "scripts/dev/mvn21.sh",
    "scripts/dev/maven-repository.sh", "scripts/dev/prepare-maven-dependencies.py", MAVEN_PREPARATION,
)
MAVEN_ORCHESTRATION = ".github/workflows/ci.yml"
MAVEN_CONSUMER_WORKFLOWS = {
    name: f".github/workflows/{name}.yml"
    for name in (
        "j03", "j04", "j05", "j06", "j07", "j08", "j10", "j11",
        "j23", "j24", "j36-proof", "m1", "m3", "m4", "m5",
    )
}
INDEPENDENT_CI_WORKFLOWS = {"j09": ".github/workflows/j09.yml"}
CI_WORKFLOW_CALLS = {**MAVEN_CONSUMER_WORKFLOWS, **INDEPENDENT_CI_WORKFLOWS}
CI_WORKFLOW_CALL_ORDER = (
    "j03", "j04", "j05", "j06", "j07", "j08", "j09", "j10",
    "j11", "j23", "j24", "j36-proof", "m1", "m3", "m4", "m5",
)
MAVEN_CONSUMER_JOB_COUNTS = {name: 1 for name in MAVEN_CONSUMER_WORKFLOWS}
MAVEN_CONSUMER_JOB_COUNTS.update({"m3": 7, "m4": 8, "m5": 2})
MAVEN_VALIDATION_JOB_COUNT = 29

# Keep CI Action pins outside the Maven dependency lock so they do not invalidate its cache key.


def approved_action_pins(root, dependency_lock):
    ci_lock = json.loads((root / "build/ci-action-lock.json").read_text())
    if ci_lock.get("schemaVersion") != 1 or not isinstance(ci_lock.get("actions"), dict):
        raise ValueError("CI Action lock has an unsupported schema")
    dependency_actions = dependency_lock["actions"]
    ci_actions = ci_lock["actions"]
    if set(dependency_actions).intersection(ci_actions):
        raise ValueError("CI Action pins must not be duplicated across lock files")
    pins = {**dependency_actions, **ci_actions}
    if not pins or any(not isinstance(name, str) or not isinstance(revision, str)
                       or not re.fullmatch(r"[0-9a-f]{40}", revision)
                       for name, revision in pins.items()):
        raise ValueError("CI Actions must use complete immutable SHA pins")
    return pins


def workflow_actions(content, root, pins):
    for action in re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", content, re.MULTILINE):
        if action.startswith("./"):
            workflow_directory = (root / ".github/workflows").resolve()
            local_workflow = (root / action[2:]).resolve()
            if (local_workflow.parent != workflow_directory
                    or local_workflow.suffix != ".yml"
                    or not local_workflow.is_file()
                    or not re.search(r"(?m)^  workflow_call:\s*$", local_workflow.read_text())):
                raise ValueError("local workflow calls must target an audited reusable workflow")
            continue
        name, separator, revision = action.partition("@")
        if not separator or not re.fullmatch(r"[0-9a-f]{40}", revision) or pins.get(name) != revision:
            raise ValueError("CI action is not an approved immutable pin")


def workflow_job_blocks(content):
    jobs = content.partition("jobs:\n")[2]
    return {
        match.group(1): match.group(0)
        for match in re.finditer(
            r"^  ([A-Za-z_][\w-]*):\n.*?(?=^  [A-Za-z_][\w-]*:|\Z)",
            jobs,
            re.MULTILINE | re.DOTALL,
        )
    }


def expected_status_env(job_id):
    return re.sub(r"[^A-Za-z0-9]+", "_", job_id).upper() + "_RESULT"


def require_status_summary(block, job_ids, label):
    dollar = chr(36)
    expected_needs = "    needs:\n" + "".join(f"      - {job_id}\n" for job_id in job_ids)
    if expected_needs not in block:
        raise ValueError(f"{label} summary must depend on every expected job")
    if "    if: " + dollar + "{{ always() && !cancelled() }}\n" not in block:
        raise ValueError(f"{label} summary must run after failures unless cancelled")
    for job_id in job_ids:
        expected = (
            f"          {expected_status_env(job_id)}: "
            + dollar + "{{ needs." + job_id + ".result }}\n"
        )
        if expected not in block:
            raise ValueError(f"{label} summary is missing {job_id} result")
    if 'if [[ "$result" != success ]]; then' not in block or 'exit "$failed"' not in block:
        raise ValueError(f"{label} summary must fail when a job fails or is skipped")


def require_ci_status_summary(block, job_ids):
    dollar = chr(36)
    expected_needs = "    needs:\n" + "".join(f"      - {job_id}\n" for job_id in job_ids)
    if expected_needs not in block:
        raise ValueError("CI summary must depend on the plan and every validation job")
    if "    if: " + dollar + "{{ always() && !cancelled() }}\n" not in block:
        raise ValueError("CI summary must run after failures unless cancelled")

    expected_environment = {
        "PLAN_RESULT": dollar + "{{ needs.plan.result }}",
        "SELECTED_GROUPS": dollar + "{{ needs.plan.outputs.groups }}",
        "RUN_MAVEN": dollar + "{{ needs.plan.outputs.maven }}",
        "RUN_DOCS": dollar + "{{ needs.plan.outputs.docs }}",
        "MAVEN_RESULT": dollar + "{{ needs.maven-dependencies.result }}",
        "DOCS_RESULT": dollar + "{{ needs.docs-validation.result }}",
    }
    for group in CI_WORKFLOW_CALL_ORDER:
        result_name = "RESULT_" + re.sub(r"[^A-Za-z0-9]+", "_", group).upper()
        expected_environment[result_name] = dollar + "{{ needs." + group + ".result }}"
    for name, value in expected_environment.items():
        if f"          {name}: {value}\n" not in block:
            raise ValueError(f"CI summary is missing {name}")
    if "run: python3 scripts/verify/ci_impact.py --check-results\n" not in block:
        raise ValueError("CI summary must validate selected and skipped job results")


def multi_job_summary_policy(workflow, content, validation_job_ids):
    required = {
        "m3.yml": ["j28-history", "j30", "j31", "j32", "j33", "j34", "j35"],
        "m4.yml": ["j40", "j41", "j42", "j43", "j44", "j45", "j46", "j47"],
        "m5.yml": ["j50", "j51"],
    }
    if workflow.name not in required:
        return
    if validation_job_ids != required[workflow.name]:
        raise ValueError(f"{workflow.name} validation job set changed")
    summary = workflow_job_blocks(content).get("summary")
    if not summary:
        raise ValueError(f"{workflow.name} is missing its inner-job summary")
    require_status_summary(summary, validation_job_ids, workflow.name)


def maven_cache_policy(workflow, content):
    jobs = workflow_job_blocks(content)
    cache_jobs = [
        (job_id, block)
        for job_id, block in jobs.items()
        if "          cache: maven\n" in block
    ]
    if not cache_jobs:
        return 0

    if workflow.name == Path(MAVEN_PREPARATION).name:
        if len(cache_jobs) != 1 or cache_jobs[0][0] != "prepare":
            raise ValueError("Maven cache must have exactly one preparation job")
        block = cache_jobs[0][1]
        if ("run: python3 scripts/verify/test_maven_resilience.py" not in block
                or "run: python3 scripts/dev/prepare-maven-dependencies.py" not in block
                or "if: steps.java-cache.outputs.cache-hit != 'true'" not in block
                or "cache-read-only: true" in block):
            raise ValueError("Maven cache must be tested and populated before it is saved")
        return 1

    group = Path(workflow).stem
    if group not in MAVEN_CONSUMER_JOB_COUNTS:
        raise ValueError("unexpected Maven cache consumer workflow")
    input_declaration = (
        "  workflow_call:\n"
        "    inputs:\n"
        "      maven_preparation_result:\n"
        "        description: Result of the shared Maven preparation job.\n"
        "        required: true\n"
        "        type: string\n"
    )
    if input_declaration not in content:
        raise ValueError("Maven consumers must require the shared preparation result")

    dollar = chr(36)
    validation_job_ids = []
    for job_id, block in cache_jobs:
        match = re.search(
            r"^          cache-dependency-path: \|\n((?:            \S[^\n]*\n)+)",
            block,
            re.MULTILINE,
        )
        if not match or tuple(line.strip() for line in match[1].splitlines()) != MAVEN_CACHE_INPUTS:
            raise ValueError("Maven cache producer and consumers must use the same complete input key")
        validation_job_ids.append(job_id)
        required = (
            "    if: " + dollar + "{{ !cancelled() }}\n",
            "          PREPARATION_RESULT: " + dollar + "{{ inputs.maven_preparation_result }}\n",
            '        run: test "$PREPARATION_RESULT" = success\n',
            "          cache-read-only: true\n",
        )
        if any(item not in block for item in required) or "    needs:" in block:
            raise ValueError("Maven test jobs must run in parallel, require preparation, and read the cache only")
        gate = block.find('run: test "$PREPARATION_RESULT" = success')
        checkout = block.find("uses: actions/checkout@")
        if gate < 0 or (checkout >= 0 and gate > checkout):
            raise ValueError("Maven preparation must be checked before test setup")
    if len(cache_jobs) != MAVEN_CONSUMER_JOB_COUNTS[group]:
        raise ValueError(f"{group} Maven validation job count changed")
    multi_job_summary_policy(workflow, content, validation_job_ids)
    return len(cache_jobs)


def locked_jdk_archive_policy(workflow, content):
    """Require every pinned JDK setup to restore and verify its locked archive."""
    jobs = workflow_job_blocks(content)
    setup_jobs = [
        (job_id, block)
        for job_id, block in jobs.items()
        if "distribution: jdkfile\n" in block
    ]
    for job_id, block in setup_jobs:
        required = (
            "id: jdk-cache\n",
            "uses: actions/cache@",
            "path: ${{ runner.temp }}/locked-temurin-jdk.tar.gz\n",
            "key: locked-jdk21-${{ runner.os }}-${{ hashFiles('build/dependency-lock.json') }}\n",
            "if: steps.jdk-cache.outputs.cache-hit != 'true'\n",
            'run: python3 scripts/dev/download-locked-jdk.py --output "$RUNNER_TEMP/locked-temurin-jdk.tar.gz"\n',
            'run: python3 scripts/dev/download-locked-jdk.py --verify "$RUNNER_TEMP/locked-temurin-jdk.tar.gz"\n',
            "java-version: '21'\n",
            "jdk-file: ${{ runner.temp }}/locked-temurin-jdk.tar.gz\n",
        )
        if any(item not in block for item in required):
            raise ValueError(f"{workflow.name}:{job_id} must cache and verify the locked JDK archive")
    return len(setup_jobs)


def ci_orchestration_policy(root, content=None):
    path = root / MAVEN_ORCHESTRATION
    content = content if content is not None else path.read_text()
    dollar = chr(36)
    expected_events = (
        "  push:\n"
        "    branches:\n"
        "      - main\n"
        "  pull_request:\n"
        "    branches:\n"
        "      - main\n"
        "  merge_group:\n"
        "    types:\n"
        "      - checks_requested"
    )
    events = content.partition("on:\n")[2].partition("\n\npermissions:\n")[0]
    if events != expected_events:
        raise ValueError("CI must run for main pushes, main PRs, and merge queue checks only")

    reusable_workflows = {MAVEN_PREPARATION, *CI_WORKFLOW_CALLS.values()}
    for reusable_workflow in reusable_workflows:
        reusable_content = (root / reusable_workflow).read_text()
        reusable_events = reusable_content.partition("on:\n")[2].partition("\n\npermissions:\n")[0]
        triggers = re.findall(r"(?m)^  ([A-Za-z_][\w-]*):[ \t]*$", reusable_events)
        if triggers != ["workflow_call"]:
            raise ValueError(f"{reusable_workflow} must run only through the CI orchestrator")

    jobs = workflow_job_blocks(content)
    expected_job_ids = {"plan", "maven-dependencies", "docs-validation", "ci-result", *CI_WORKFLOW_CALLS}
    if set(jobs) != expected_job_ids:
        raise ValueError("CI orchestration must call every expected verification workflow exactly once")

    plan = jobs["plan"]
    if ("    name: CI · 计算影响范围\n" not in plan
            or "python3 scripts/verify/test_ci_impact.py\n" not in plan
            or "python3 scripts/verify/ci_impact.py" not in plan
            or "      groups: " + dollar + "{{ steps.selection.outputs.groups }}\n" not in plan):
        raise ValueError("CI must test and publish the impact selection before scheduling validation")

    preparation = jobs["maven-dependencies"]
    if ("    needs: plan\n" not in preparation
            or "needs.plan.outputs.maven == 'true'" not in preparation
            or f"    uses: ./{MAVEN_PREPARATION}\n" not in preparation):
        raise ValueError("CI must call Maven preparation once when a selected check needs it")
    for job_id, workflow in MAVEN_CONSUMER_WORKFLOWS.items():
        block = jobs[job_id]
        required = (
            "    needs: [plan, maven-dependencies]\n",
            "needs.plan.result == 'success'",
            "needs.maven-dependencies.result == 'success'",
            "contains(fromJSON(needs.plan.outputs.groups), '" + job_id + "')",
            f"    uses: ./{workflow}\n",
            "    with:\n"
            "      maven_preparation_result: " + dollar + "{{ needs.maven-dependencies.result }}\n",
        )
        if any(item not in block for item in required):
            raise ValueError(f"{job_id} must run only when selected after shared Maven preparation")
    independent = jobs["j09"]
    if ("    needs: plan\n" not in independent
            or "contains(fromJSON(needs.plan.outputs.groups), 'j09')" not in independent
            or f"    uses: ./{INDEPENDENT_CI_WORKFLOWS['j09']}\n" not in independent
            or "maven_preparation_result:" in independent):
        raise ValueError("J09 must run only when selected and independently of Maven preparation")

    docs = jobs["docs-validation"]
    if ("    needs: plan\n" not in docs
            or "needs.plan.outputs.docs == 'true'" not in docs
            or "git diff --check" not in docs):
        raise ValueError("CI must run the document diff check only when documents changed")

    summary = jobs["ci-result"]
    expected_summary_jobs = ["plan", "maven-dependencies", "docs-validation", *CI_WORKFLOW_CALL_ORDER]
    require_ci_status_summary(summary, expected_summary_jobs)
    return {
        "maven_preparation_calls": 1,
        "maven_consumer_workflows": len(MAVEN_CONSUMER_WORKFLOWS),
        "maven_validation_jobs": MAVEN_VALIDATION_JOB_COUNT,
        "stable_summary_gate": True,
        "j09_independent": True,
    }


def static_policy(root=ROOT):
    lock = json.loads((root / "build/dependency-lock.json").read_text())
    action_pins = approved_action_pins(root, lock)
    settings = ET.parse(root / "build/maven-settings-public.xml").getroot()
    namespace = {"m": "http://maven.apache.org/SETTINGS/1.2.0"}
    mirrors = settings.findall("m:mirrors/m:mirror", namespace)
    if len(mirrors) != 1 or mirrors[0].findtext("m:url", "", namespace) != "https://repo.maven.apache.org/maven2" or mirrors[0].findtext("m:mirrorOf", "", namespace) != "*" or settings.findall("m:servers", namespace):
        raise ValueError("public Maven settings must use only Central without credentials")
    runtime = lock["runtimeImage"]
    if runtime["javaRelease"] != lock["jdk"]["release"] or runtime["platform"] != "linux/amd64" or not re.fullmatch(r"sha256:[0-9a-f]{64}", runtime["platformDigest"]) or not runtime["reference"].endswith("@" + runtime["platformDigest"]):
        raise ValueError("runtime platform and locked JDK patch differ")
    if (root / "server/src/main/docker/Dockerfile.m0").read_text().splitlines()[0] != "FROM " + runtime["reference"]:
        raise ValueError("Dockerfile does not use the exact locked image")
    workflows = sorted((root / ".github/workflows").glob("*.yml"))
    if not workflows:
        raise ValueError("public verification workflows are missing")
    preparation_jobs = 0
    consumer_jobs = 0
    locked_jdk_jobs = 0
    for workflow in workflows:
        content = workflow.read_text()
        if "pull_request_target" in content or re.search(r"secrets\s*[.\[]", content) or re.search(r"permissions\s*:\s*(?:write-all|[\s\S]*?contents\s*:\s*write)", content):
            raise ValueError("M0 public CI must not request target secrets or write access")
        workflow_actions(content, root, action_pins)
        cache_jobs = maven_cache_policy(workflow, content)
        locked_jdk_jobs += locked_jdk_archive_policy(workflow, content)
        if workflow.name == Path(MAVEN_PREPARATION).name:
            preparation_jobs += cache_jobs
        else:
            consumer_jobs += cache_jobs
        checkout_lines = content.splitlines()
        for index, line in enumerate(checkout_lines):
            match = re.match(r"^([ \t]*)-[ \t]*uses:[ \t]*actions/checkout@", line)
            if not match:
                continue
            indent = len(match.group(1))
            block = [line]
            for next_line in checkout_lines[index + 1:]:
                next_step = re.match(r"^([ \t]*)-[ \t]", next_line)
                if next_step and len(next_step.group(1)) == indent:
                    break
                if next_line.strip() and len(next_line) - len(next_line.lstrip(" \t")) < indent:
                    break
                block.append(next_line)
            if "persist-credentials: false" not in "\n".join(block):
                raise ValueError("checkout credentials must not persist")
    if preparation_jobs != 1 or consumer_jobs != MAVEN_VALIDATION_JOB_COUNT:
        raise ValueError(f"CI must keep one Maven preparation job and all {MAVEN_VALIDATION_JOB_COUNT} Maven validation jobs")
    if locked_jdk_jobs != MAVEN_VALIDATION_JOB_COUNT + 2:
        raise ValueError("every Java validation job must restore the locked JDK archive")
    orchestration = ci_orchestration_policy(root)
    return {"workflows": len(workflows), "immutable_action_pins": len(action_pins),
            "runtime_base": runtime["reference"], "public_maven_only": True,
            "maven_preparation_jobs": preparation_jobs,
            "maven_preparation_calls": orchestration["maven_preparation_calls"],
            "maven_consumer_workflows": orchestration["maven_consumer_workflows"],
            "maven_validation_jobs": consumer_jobs,
            "locked_jdk_archive_jobs": locked_jdk_jobs,
            "stable_summary_gate": orchestration["stable_summary_gate"],
            "j09_independent": orchestration["j09_independent"]}


def actual_go_policy(root=ROOT):
    result = subprocess.run([str(root / "scripts/dev/go-m0.sh"), "env", "-json", *GO_POLICY],
                            cwd=root, text=True, capture_output=True, timeout=300, check=True)
    environment = json.loads(result.stdout)
    if environment != GO_POLICY:
        raise ValueError("actual Go environment differs from public verification policy")
    return environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    result = static_policy()
    result["actual_go_environment"] = actual_go_policy()
    arguments.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, ET.ParseError, subprocess.SubprocessError):
        raise SystemExit("public build policy rejected") from None
