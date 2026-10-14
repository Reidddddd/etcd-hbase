#!/usr/bin/env python3
"""Check Maven pins against the project POMs and cached artifact digests."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POM_NS = "http://maven.apache.org/POM/4.0.0"
NS = {"m": POM_NS}
PROPERTY = re.compile(r"\$\{([^}]+)}")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SUPPORTED_LIBRARY_ENTRIES = {
    "convergenceOverrides",
    "grpc",
    "guava",
    "hadoop",
    "hbase",
    "hbaseTestingUtil",
    "jackson",
    "javaxAnnotation",
    "protobuf",
    "slf4j",
    "snakeYaml",
    "tests",
}


class LockError(Exception):
    pass


@dataclass(frozen=True)
class Artifact:
    coordinate: str
    version: str
    sha256: str
    extension: str = "jar"
    classifier: str | None = None

    @property
    def relative_path(self) -> Path:
        group, artifact = self.coordinate.split(":", 1)
        filename = f"{artifact}-{self.version}"
        if self.classifier:
            filename += f"-{self.classifier}"
        filename += f".{self.extension}"
        return Path(*group.split("."), artifact, self.version, filename)


def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise LockError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_lock(path: Path) -> dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys)
    except (OSError, json.JSONDecodeError, LockError) as exc:
        raise LockError(f"cannot read dependency lock {path}: {exc}") from exc


def text(node: ET.Element | None) -> str | None:
    if node is None or node.text is None:
        return None
    value = node.text.strip()
    return value or None


def resolve(value: str | None, properties: dict[str, str], context: str) -> str | None:
    if value is None:
        return None
    resolved = value
    for _ in range(12):
        match = PROPERTY.search(resolved)
        if not match:
            return resolved
        name = match.group(1)
        if name not in properties:
            raise LockError(f"unresolved Maven property ${{{name}}} in {context}")
        resolved = resolved[: match.start()] + properties[name] + resolved[match.end() :]
    raise LockError(f"recursive Maven property in {context}: {value}")


def coordinate_parts(value: str) -> tuple[str, str, str | None, str | None]:
    fields = value.split(":")
    if len(fields) == 2:
        return fields[0], fields[1], None, None
    if len(fields) == 3:
        return fields[0], fields[1], fields[2], None
    if len(fields) == 4:
        return fields[0], fields[1], None, fields[3]
    raise LockError(f"unsupported Maven coordinate: {value}")


def expected_classifier(platform: str) -> str:
    return "osx-aarch_64" if platform == "macos-aarch_64" else platform


def validate_native_artifact(coordinate: str, group: str, artifact: str, platform: str, label: str) -> None:
    fields = coordinate.split(":")
    if len(fields) != 4 or fields[2] != "exe":
        raise LockError(f"{label} must use an executable Maven coordinate: {coordinate}")
    if (fields[0], fields[1]) != (group, artifact):
        raise LockError(f"{label} has an unexpected artifact coordinate: {coordinate}")
    if fields[3] != expected_classifier(platform):
        raise LockError(f"{label} classifier does not match {platform}: {coordinate}")


def read_project(root_pom: Path) -> tuple[list[Path], dict[str, str], dict[tuple[str, str], list[str]], dict[tuple[str, str], list[str]], dict[tuple[str, str], list[str]], dict[tuple[str, str], list[str]]]:
    try:
        root_tree = ET.parse(root_pom)
    except (ET.ParseError, OSError) as exc:
        raise LockError(f"cannot parse {root_pom}: {exc}") from exc
    root = root_tree.getroot()
    properties_node = root.find("m:properties", NS)
    properties = {
        child.tag.rsplit("}", 1)[-1]: (child.text or "").strip()
        for child in (list(properties_node) if properties_node is not None else [])
    }
    root_group = text(root.find("m:groupId", NS)) or text(root.find("m:parent/m:groupId", NS)) or ""
    root_version = text(root.find("m:version", NS)) or text(root.find("m:parent/m:version", NS)) or ""
    properties.setdefault("project.groupId", root_group)
    properties.setdefault("pom.groupId", root_group)
    properties.setdefault("project.version", root_version)
    properties.setdefault("pom.version", root_version)
    locked_version_properties: set[str] = set()
    for version_node in root.findall(".//m:version", NS):
        locked_version_properties.update(PROPERTY.findall(text(version_node) or ""))
    pending_properties = list(locked_version_properties)
    while pending_properties:
        property_name = pending_properties.pop()
        for referenced_name in PROPERTY.findall(properties.get(property_name, "")):
            if referenced_name not in locked_version_properties:
                locked_version_properties.add(referenced_name)
                pending_properties.append(referenced_name)
    poms = [root_pom]
    modules = root.findall("m:modules/m:module", NS)
    for module in modules:
        name = text(module)
        if name:
            module_pom = root_pom.parent / name / "pom.xml"
            if not module_pom.is_file():
                raise LockError(f"declared Maven module has no POM: {module_pom}")
            poms.append(module_pom)

    managed_dependencies: dict[tuple[str, str], list[str]] = {}
    declared_dependencies: dict[tuple[str, str], list[str]] = {}
    managed_plugins: dict[tuple[str, str], list[str]] = {}
    declared_plugins: dict[tuple[str, str], list[str]] = {}

    def identity(element: ET.Element, default_group: str) -> tuple[str, str] | None:
        group = text(element.find("m:groupId", NS)) or default_group
        artifact = text(element.find("m:artifactId", NS))
        return (group, artifact) if artifact else None

    for pom in poms:
        try:
            project = ET.parse(pom).getroot()
        except (ET.ParseError, OSError) as exc:
            raise LockError(f"cannot parse {pom}: {exc}") from exc
        override_property_nodes = []
        if pom != root_pom:
            override_property_nodes.append(project.find("m:properties", NS))
        override_property_nodes.extend(project.findall("m:profiles/m:profile/m:properties", NS))
        if override_property_nodes:
            override_property_names = {
                child.tag.rsplit("}", 1)[-1]
                for properties_node in override_property_nodes
                if properties_node is not None
                for child in list(properties_node)
            }
            property_overrides = sorted(locked_version_properties & override_property_names)
            if property_overrides:
                scope = "root" if pom == root_pom else "module"
                raise LockError(
                    f"{scope} overrides locked version property "
                    f"{', '.join(property_overrides)} in {pom}"
                )
        for dependency in project.findall("m:dependencyManagement/m:dependencies/m:dependency", NS):
            key = identity(dependency, "")
            version = resolve(text(dependency.find("m:version", NS)), properties, str(pom))
            if key and version:
                managed_dependencies.setdefault(key, []).append(version)
        for dependency in project.findall("m:dependencies/m:dependency", NS):
            key = identity(dependency, "")
            version = resolve(text(dependency.find("m:version", NS)), properties, str(pom))
            if key:
                declared_dependencies.setdefault(key, []).append(version or "<managed>")
        for plugin in project.findall("m:build/m:pluginManagement/m:plugins/m:plugin", NS):
            key = identity(plugin, "org.apache.maven.plugins")
            version = resolve(text(plugin.find("m:version", NS)), properties, str(pom))
            if key and version:
                managed_plugins.setdefault(key, []).append(version)
        for plugin in project.findall("m:build/m:plugins/m:plugin", NS):
            key = identity(plugin, "org.apache.maven.plugins")
            version = resolve(text(plugin.find("m:version", NS)), properties, str(pom))
            if key and version:
                declared_plugins.setdefault(key, []).append(version)
    return poms, properties, managed_dependencies, declared_dependencies, managed_plugins, declared_plugins


def add_artifact(output: list[Artifact], coordinate: str, sha: object, *, version: str | None = None, extension: str | None = None, classifier: str | None = None) -> None:
    if not isinstance(sha, str) or not SHA256.fullmatch(sha):
        raise LockError(f"invalid SHA-256 for {coordinate}: {sha!r}")
    group, artifact, embedded_version, embedded_classifier = coordinate_parts(coordinate)
    actual_version = embedded_version or version
    if not actual_version:
        raise LockError(f"version missing for artifact {coordinate}")
    actual_classifier = classifier or embedded_classifier
    actual_extension = extension or ("exe" if embedded_classifier is not None else "jar")
    if actual_extension == "pom":
        actual_classifier = None
    output.append(Artifact(f"{group}:{artifact}", actual_version, sha, actual_extension, actual_classifier))


def collect_artifacts(lock: dict[str, object]) -> list[Artifact]:
    artifacts: list[Artifact] = []
    plugins = lock.get("plugins", {})
    if isinstance(plugins, dict):
        for name, record in plugins.items():
            if isinstance(record, dict):
                add_artifact(artifacts, str(record.get("coordinate", "")), record.get("sha256"))

    libraries = lock.get("libraries", {})
    if isinstance(libraries, dict):
        for name in (
            "hbase",
            "hadoop",
            "hbaseTestingUtil",
            "guava",
            "slf4j",
            "snakeYaml",
            "javaxAnnotation",
        ):
            record = libraries.get(name)
            if isinstance(record, dict):
                add_artifact(artifacts, str(record.get("coordinate", "")), record.get("sha256"))
        grpc = libraries.get("grpc")
        if isinstance(grpc, dict):
            bom = str(grpc.get("bom", ""))
            if grpc.get("bomSha256"):
                add_artifact(artifacts, bom, grpc["bomSha256"], extension="pom")
            _, _, grpc_version, _ = coordinate_parts(bom)
            for name, digest in dict(grpc.get("artifacts", {})).items():
                add_artifact(artifacts, f"io.grpc:{name}:{grpc_version}", digest)
        protobuf = libraries.get("protobuf")
        if isinstance(protobuf, dict):
            add_artifact(artifacts, str(protobuf.get("javaCoordinate", "")), protobuf.get("javaSha256"))
            _, _, protoc_version, _ = coordinate_parts(str(protobuf.get("protocCoordinate", "")))
            for platform, digest in dict(protobuf.get("protocSha256", {})).items():
                classifier = expected_classifier(platform)
                add_artifact(artifacts, "com.google.protobuf:protoc", digest, version=protoc_version, extension="exe", classifier=classifier)
        overrides = libraries.get("convergenceOverrides")
        if isinstance(overrides, dict):
            for coordinate, digest in dict(overrides.get("resolvedVersions", {})).items():
                add_artifact(artifacts, coordinate, digest)
        jackson = libraries.get("jackson")
        if isinstance(jackson, dict):
            for coordinate, digest in dict(jackson.get("artifacts", {})).items():
                add_artifact(artifacts, f"{coordinate}:{jackson.get('version')}", digest)
        tests = libraries.get("tests")
        if isinstance(tests, dict):
            for name in ("junitJupiter", "mockito", "assertj"):
                record = tests.get(name)
                if isinstance(record, dict):
                    add_artifact(artifacts, str(record.get("coordinate", "")), record.get("sha256"))

    native_tools = lock.get("nativeTools", {})
    if isinstance(native_tools, dict):
        for tool_name, record in native_tools.items():
            if not isinstance(record, dict):
                continue
            version = str(record.get("version", ""))
            platform_artifacts = record.get("platformArtifacts", {})
            if not isinstance(platform_artifacts, dict):
                continue
            for platform, value in platform_artifacts.items():
                if isinstance(value, dict) and value.get("sha256"):
                    add_artifact(artifacts, str(value.get("coordinate", "")), value.get("sha256"), version=version)
    return artifacts


def validate_metadata(lock: dict[str, object], root_pom: Path) -> int:
    poms, properties, managed_dependencies, declared_dependencies, managed_plugins, declared_plugins = read_project(root_pom)
    errors: list[str] = []

    plugins = lock.get("plugins")
    if not isinstance(plugins, dict):
        raise LockError("lock.plugins must be an object")
    for name, record in plugins.items():
        if not isinstance(record, dict):
            errors.append(f"plugins.{name} must be an object")
            continue
        coordinate = str(record.get("coordinate", ""))
        try:
            group, artifact, version, _ = coordinate_parts(coordinate)
        except LockError as exc:
            errors.append(str(exc))
            continue
        if record.get("version") != version:
            errors.append(f"plugins.{name}.version disagrees with {coordinate}")
        actual = managed_plugins.get((group, artifact), [])
        if version not in actual:
            errors.append(f"POM pluginManagement does not pin {coordinate}; found {actual or 'no declaration'}")
        if any(actual_version != version for actual_version in actual):
            errors.append(f"a module overrides locked pluginManagement {coordinate} with {actual}")
        explicit_versions = declared_plugins.get((group, artifact), [])
        if any(explicit_version != version for explicit_version in explicit_versions):
            errors.append(f"a module overrides locked plugin {coordinate} with {explicit_versions}")

    libraries = lock.get("libraries")
    if not isinstance(libraries, dict):
        raise LockError("lock.libraries must be an object")
    unsupported_libraries = sorted(set(libraries) - SUPPORTED_LIBRARY_ENTRIES)
    if unsupported_libraries:
        errors.append(f"unsupported libraries entries: {unsupported_libraries}")

    def require_managed(coordinate: str, label: str) -> None:
        try:
            group, artifact, version, _ = coordinate_parts(coordinate)
        except LockError as exc:
            errors.append(f"{label}: {exc}")
            return
        actual = managed_dependencies.get((group, artifact), [])
        if version not in actual:
            errors.append(f"{label}: POM dependencyManagement does not pin {coordinate}; found {actual or 'no declaration'}")
        if any(actual_version != version for actual_version in actual):
            errors.append(f"{label}: a module overrides locked dependencyManagement {coordinate} with {actual}")
        explicit_versions = declared_dependencies.get((group, artifact), [])
        if any(explicit_version not in ("<managed>", version) for explicit_version in explicit_versions):
            errors.append(f"a module overrides locked dependency {coordinate} with {explicit_versions}")

    def require_declared(group: str, artifact: str, version: str, label: str, *, managed_by: tuple[str, str, str] | None = None) -> None:
        key = (group, artifact)
        if key not in declared_dependencies and key not in managed_dependencies:
            errors.append(f"{label} is not referenced by any Maven module: {group}:{artifact}")
        explicit_versions = declared_dependencies.get(key, [])
        if any(explicit_version not in ("<managed>", version) for explicit_version in explicit_versions):
            errors.append(f"{label} has a direct version that overrides {version}: {explicit_versions}")
        if managed_by:
            bom_group, bom_artifact, bom_version = managed_by
            versions = managed_dependencies.get((bom_group, bom_artifact), [])
            if bom_version not in versions:
                errors.append(f"{label} requires imported BOM {bom_group}:{bom_artifact}:{bom_version}; found {versions or 'none'}")
            if any(actual_version != bom_version for actual_version in versions):
                errors.append(f"{label} has a module override for imported BOM {bom_group}:{bom_artifact}: {versions}")
        else:
            versions = managed_dependencies.get(key, [])
            if version not in versions:
                errors.append(f"{label} version {version} is not managed by the POM; found {versions or 'none'}")
            if any(actual_version != version for actual_version in versions):
                errors.append(f"{label} has a module override for managed version {version}: {versions}")

    for name in (
        "hbase",
        "hadoop",
        "hbaseTestingUtil",
        "guava",
        "slf4j",
        "snakeYaml",
        "javaxAnnotation",
    ):
        record = libraries.get(name)
        if isinstance(record, dict) and record.get("coordinate"):
            require_managed(str(record["coordinate"]), f"libraries.{name}")

    grpc = libraries.get("grpc")
    grpc_version = None
    if isinstance(grpc, dict):
        bom = str(grpc.get("bom", ""))
        group, artifact, grpc_version, _ = coordinate_parts(bom)
        require_managed(bom, "libraries.grpc.bom")
        for artifact_name in dict(grpc.get("artifacts", {})):
            require_declared(group, artifact_name, grpc_version, f"libraries.grpc.artifacts.{artifact_name}", managed_by=(group, artifact, grpc_version))

    protobuf = libraries.get("protobuf")
    if isinstance(protobuf, dict):
        require_managed(str(protobuf.get("javaCoordinate", "")), "libraries.protobuf.javaCoordinate")
        group, artifact, version, _ = coordinate_parts(str(protobuf.get("javaCoordinate", "")))
        protoc_group, protoc_artifact, protoc_version, _ = coordinate_parts(str(protobuf.get("protocCoordinate", "")))
        if (group, version) != (protoc_group, protoc_version):
            errors.append("libraries.protobuf Java and protoc coordinates must share group and version")
        native_protoc = lock.get("nativeTools", {}).get("protoc", {})
        if isinstance(native_protoc, dict):
            if native_protoc.get("version") != version:
                errors.append("nativeTools.protoc.version disagrees with libraries.protobuf.javaCoordinate")
            native_platforms = dict(native_protoc.get("platformArtifacts", {}))
            protoc_hash_platforms = dict(protobuf.get("protocSha256", {}))
            if set(native_platforms) != set(protoc_hash_platforms):
                errors.append("nativeTools.protoc platforms disagree with libraries.protobuf.protocSha256 platforms")
            for platform, coordinate in native_platforms.items():
                try:
                    validate_native_artifact(str(coordinate), protoc_group, protoc_artifact, platform, f"nativeTools.protoc.platformArtifacts.{platform}")
                except LockError as exc:
                    errors.append(str(exc))
            for platform, digest in protoc_hash_platforms.items():
                if not isinstance(digest, str) or not SHA256.fullmatch(digest):
                    errors.append(f"libraries.protobuf.protocSha256.{platform} is not a SHA-256 digest")

    grpc_generator = lock.get("nativeTools", {}).get("protocGenGrpcJava", {})
    if isinstance(grpc_generator, dict) and grpc_version and grpc_generator.get("version") != grpc_version:
        errors.append("nativeTools.protocGenGrpcJava.version disagrees with libraries.grpc.bom")
    if isinstance(grpc_generator, dict):
        for platform, record in dict(grpc_generator.get("platformArtifacts", {})).items():
            if not isinstance(record, dict):
                errors.append(f"nativeTools.protocGenGrpcJava.platformArtifacts.{platform} has no checksum record")
                continue
            try:
                validate_native_artifact(str(record.get("coordinate", "")), "io.grpc", "protoc-gen-grpc-java", platform, f"nativeTools.protocGenGrpcJava.platformArtifacts.{platform}")
                if not isinstance(record.get("sha256"), str) or not SHA256.fullmatch(record["sha256"]):
                    errors.append(f"nativeTools.protocGenGrpcJava.platformArtifacts.{platform}.sha256 is invalid")
            except LockError as exc:
                errors.append(str(exc))

    java_release = properties.get("maven.compiler.release")
    if java_release != "21":
        errors.append(f"POM maven.compiler.release must match the JDK 21 project baseline; found {java_release!r}")

    overrides = libraries.get("convergenceOverrides")
    if isinstance(overrides, dict):
        for coordinate in dict(overrides.get("resolvedVersions", {})):
            require_managed(coordinate, f"libraries.convergenceOverrides.{coordinate}")

    jackson = libraries.get("jackson")
    if isinstance(jackson, dict):
        jackson_version = str(jackson.get("version", ""))
        for ga in dict(jackson.get("artifacts", {})):
            require_managed(f"{ga}:{jackson_version}", f"libraries.jackson.artifacts.{ga}")

    tests = libraries.get("tests")
    if isinstance(tests, dict):
        junit = tests.get("junitJupiter")
        if isinstance(junit, dict):
            group, artifact, version, _ = coordinate_parts(str(junit.get("coordinate", "")))
            require_declared(group, artifact, version, "libraries.tests.junitJupiter", managed_by=("org.junit", "junit-bom", version))
        for name in ("mockito", "assertj"):
            record = tests.get(name)
            if isinstance(record, dict):
                require_managed(str(record.get("coordinate", "")), f"libraries.tests.{name}")

    if lock.get("hbase", {}).get("public", {}).get("version") != libraries.get("hbase", {}).get("coordinate", "").rsplit(":", 1)[-1]:
        errors.append("hbase.public.version disagrees with libraries.hbase.coordinate")
    if lock.get("hbase", {}).get("public", {}).get("hadoopVersion") != libraries.get("hadoop", {}).get("coordinate", "").rsplit(":", 1)[-1]:
        errors.append("hbase.public.hadoopVersion disagrees with libraries.hadoop.coordinate")

    maven = lock.get("maven", {})
    wrapper_properties = root_pom.parent / ".mvn" / "wrapper" / "maven-wrapper.properties"
    if isinstance(maven, dict):
        if not wrapper_properties.is_file():
            errors.append("Maven Wrapper properties are missing")
        else:
            values = {}
            for line in wrapper_properties.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    values[key.strip()] = value.strip()
            expected_url = str(maven.get("distributionUrl", ""))
            expected_sha = str(maven.get("distributionSha256", ""))
            if values.get("distributionUrl") != expected_url:
                errors.append("Maven Wrapper distributionUrl disagrees with dependency lock")
            if values.get("distributionSha256Sum") != expected_sha:
                errors.append("Maven Wrapper distributionSha256Sum disagrees with dependency lock")
            if values.get("wrapperVersion") != str(maven.get("wrapperPlugin", "")):
                errors.append("Maven Wrapper version disagrees with dependency lock")
            wrapper_jar = maven.get("wrapperJar", {})
            wrapper_jar_path = root_pom.parent / ".mvn" / "wrapper" / "maven-wrapper.jar"
            if isinstance(wrapper_jar, dict):
                expected_wrapper_sha = str(wrapper_jar.get("sha256", ""))
                if not wrapper_jar_path.is_file():
                    errors.append("committed Maven Wrapper JAR is missing")
                elif hashlib.sha256(wrapper_jar_path.read_bytes()).hexdigest() != expected_wrapper_sha:
                    errors.append("committed Maven Wrapper JAR SHA-256 disagrees with dependency lock")

    if errors:
        raise LockError("Maven lock metadata mismatch:\n  - " + "\n  - ".join(errors))
    return len(poms)


def repository_from_args(args: argparse.Namespace) -> Path:
    configured = args.repository or args.maven_repo_local or None
    if configured is None:
        configured = str(Path.home() / ".m2" / "repository")
    return Path(configured).expanduser().resolve()


def verify_artifacts(artifacts: list[Artifact], repository: Path, require_all: bool) -> tuple[int, int]:
    failures: list[str] = []
    checked = 0
    missing = 0
    seen: dict[Path, str] = {}
    for artifact in artifacts:
        relative = artifact.relative_path
        previous = seen.get(relative)
        if previous and previous != artifact.sha256:
            failures.append(f"lock entries disagree for {relative}")
            continue
        seen[relative] = artifact.sha256
        path = repository / relative
        if not path.is_file():
            missing += 1
            if require_all:
                failures.append(f"locked artifact is not cached: {relative}")
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != artifact.sha256:
            failures.append(f"SHA-256 mismatch for {relative}: expected {artifact.sha256}, got {digest}")
        else:
            checked += 1
    if failures:
        raise LockError("Maven artifact lock mismatch:\n  - " + "\n  - ".join(failures))
    return checked, missing


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT, help="project root")
    parser.add_argument("--lock", type=Path, help="alternate lock file for negative checks")
    parser.add_argument("--repository", type=Path, help="Maven local repository")
    parser.add_argument("--maven-repo-local", help="value passed via -Dmaven.repo.local")
    parser.add_argument("--mode", choices=("preflight", "metadata", "artifacts"), default="preflight")
    parser.add_argument("--require-all", action="store_true", help="fail when any checksummed artifact is not cached")
    args = parser.parse_args()

    root = args.root.expanduser().resolve()
    lock_path = args.lock.expanduser().resolve() if args.lock else root / "build" / "dependency-lock.json"
    root_pom = root / "pom.xml"
    try:
        lock = load_lock(lock_path)
        if args.mode != "artifacts":
            module_count = validate_metadata(lock, root_pom)
            print(f"Maven lock metadata OK: {module_count} POMs, {len(lock.get('plugins', {}))} plugin pins")
        if args.mode != "metadata":
            checked, missing = verify_artifacts(collect_artifacts(lock), repository_from_args(args), args.require_all)
            print(f"Maven artifact hashes OK: {checked} verified, {missing} not cached")
    except (LockError, OSError, KeyError, TypeError, AttributeError) as exc:
        print(f"maven-lock: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
