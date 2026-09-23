# /// script
# requires-python = ">=3.14"
# dependencies = ["packaging==26.3"]
# ///
from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Final, NamedTuple, TypedDict, cast

from packaging.requirements import Requirement

__all__: Final = ()

_PIP_VERSION: Final[str] = "26.2.1"
_PYTHON_VERSION: Final[str] = "3.14.7"


class Platform(NamedTuple):
    pip_tags: tuple[str, ...]
    uv_platform: str
    markers: dict[str, str]


_MARKERS: Final[dict[str, str]] = {
    "implementation_name": "cpython",
    "implementation_version": _PYTHON_VERSION,
    "os_name": "posix",
    "platform_python_implementation": "CPython",
    "platform_release": "",
    "platform_version": "",
    "python_full_version": _PYTHON_VERSION,
    "python_version": "3.14",
}
_PLATFORMS: Final[dict[str, Platform]] = {
    "macos-aarch64": Platform(
        ("macosx_11_0_arm64",),
        "aarch64-apple-darwin",
        _MARKERS | {"platform_machine": "arm64", "platform_system": "Darwin", "sys_platform": "darwin"},
    ),
    "linux-x86_64": Platform(
        # An explicit --platform matches only the tags given, so every glibc baseline up to 2.28 is listed.
        (*(f"manylinux_2_{minor}_x86_64" for minor in range(28, 16, -1)), "manylinux2014_x86_64"),
        "x86_64-manylinux_2_28",
        _MARKERS | {"platform_machine": "x86_64", "platform_system": "Linux", "sys_platform": "linux"},
    ),
}


class Artifact(TypedDict):
    project: str
    version: str
    filename: str
    url: str
    sha256: str
    requires_python: str | None
    dependencies: list[str]
    requested: bool
    size: int


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("platform", choices=_PLATFORMS)
    parser.add_argument("--fresh", action="store_true", help="resolve without the existing corpus pins")
    arguments = parser.parse_args()
    directory = Path("crates/peryx-ecosystem-pypi/src/bench/fixtures")
    requirements = directory / "requirements.in"
    roots = [line for line in requirements.read_text().splitlines() if line]
    corpus_path = directory / f"corpus-{arguments.platform}.json"
    platform = _PLATFORMS[arguments.platform]
    with tempfile.TemporaryDirectory() as temporary:
        constraints = Path(temporary) / "constraints.txt"
        constraints.write_text("" if arguments.fresh else previous_pins(corpus_path))
        pins = Path(temporary) / "pins.txt"
        # pip evaluates markers against the machine running it, whatever --platform says, so uv resolves for the
        # target and pip only selects each pinned wheel.
        subprocess.run(
            [
                "uv",
                "pip",
                "compile",
                str(requirements),
                f"--python-platform={platform.uv_platform}",
                f"--python-version={_PYTHON_VERSION}",
                "--only-binary=:all:",
                f"--constraints={constraints}",
                "--no-header",
                "--no-annotate",
                f"--output-file={pins}",
            ],
            check=True,
        )
        report_path = Path(temporary) / "pip-report.json"
        subprocess.run(
            [
                "uvx",
                "--from",
                f"pip=={_PIP_VERSION}",
                "pip",
                "install",
                "--dry-run",
                "--no-deps",
                "--ignore-installed",
                "--only-binary=:all:",
                "--implementation=cp",
                "--python-version=3.14",
                "--abi=cp314",
                *(f"--platform={tag}" for tag in platform.pip_tags),
                f"--report={report_path}",
                f"--requirement={pins}",
            ],
            check=True,
        )
        report = cast("dict[str, object]", json.loads(report_path.read_text()))
    entries = cast("list[dict[str, object]]", report["install"])
    dependencies = closure(
        roots,
        {
            normalize(cast("str", metadata["name"])): [
                Requirement(line) for line in cast("list[str]", metadata.get("requires_dist", []))
            ]
            for metadata in (cast("dict[str, object]", entry["metadata"]) for entry in entries)
        },
        platform.markers,
    )
    requested = {normalize(Requirement(root).name) for root in roots}
    output = {
        "schema": 1,
        "pip_version": _PIP_VERSION,
        "python": _PYTHON_VERSION,
        "platform": arguments.platform,
        "roots": roots,
        "artifacts": sorted(
            (artifact(entry, dependencies, requested) for entry in entries),
            key=lambda item: normalize(item["project"]),
        ),
    }
    corpus_path.write_text(json.dumps(output, indent=2) + "\n")


def previous_pins(corpus_path: Path) -> str:
    if not corpus_path.exists():
        return ""
    corpus = json.loads(corpus_path.read_text(encoding="utf-8"))
    return "".join(f"{entry['project']}=={entry['version']}\n" for entry in corpus["artifacts"])


def closure(
    roots: list[str], requirements: dict[str, list[Requirement]], markers: dict[str, str]
) -> dict[str, list[str]]:
    extras: dict[str, set[str]] = {}
    pending = [Requirement(root) for root in roots]
    while pending:
        requirement = pending.pop()
        if (project := normalize(requirement.name)) not in requirements:
            msg = f"the corpus has no artifact for {project}, which the target platform requires"
            raise ValueError(msg)
        if project in extras and set(requirement.extras) <= extras[project]:
            continue
        extras[project] = extras.get(project, set()) | set(requirement.extras)
        pending.extend(applicable(requirements[project], markers, extras[project]))
    if unreached := set(requirements) - set(extras):
        msg = f"the corpus holds artifacts no root requires: {sorted(unreached)}"
        raise ValueError(msg)
    return {
        project: sorted({
            normalize(dependency.name) for dependency in applicable(requirements[project], markers, active)
        })
        for project, active in extras.items()
    }


def applicable(requirements: list[Requirement], markers: dict[str, str], extras: set[str]) -> list[Requirement]:
    return [
        requirement
        for requirement in requirements
        if requirement.marker is None
        or any(requirement.marker.evaluate(markers | {"extra": extra}) for extra in {"", *extras})
    ]


def artifact(entry: dict[str, object], dependencies: dict[str, list[str]], requested: set[str]) -> Artifact:
    metadata = cast("dict[str, object]", entry["metadata"])
    project = cast("str", metadata["name"])
    download = cast("dict[str, object]", entry["download_info"])
    archive = cast("dict[str, object]", download["archive_info"])
    url = cast("str", download["url"])
    filename = urllib.parse.unquote(Path(urllib.parse.urlsplit(url).path).name)
    sha256 = cast("dict[str, str]", archive["hashes"])["sha256"]
    return Artifact(
        project=project,
        version=cast("str", metadata["version"]),
        filename=filename,
        url=url,
        sha256=sha256,
        requires_python=cast("str | None", metadata.get("requires_python")),
        dependencies=dependencies[normalize(project)],
        requested=normalize(project) in requested,
        size=artifact_size(project, filename, sha256),
    )


def artifact_size(project: str, filename: str, sha256: str) -> int:
    request = urllib.request.Request(
        f"https://pypi.org/simple/{normalize(project)}/",
        headers={"Accept": "application/vnd.pypi.simple.v1+json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        page = cast("dict[str, object]", json.load(response))
    for file in cast("list[dict[str, object]]", page["files"]):
        if file["filename"] == filename and cast("dict[str, str]", file["hashes"])["sha256"] == sha256:
            return cast("int", file["size"])
    msg = f"{project} has no {filename} with sha256 {sha256}"
    raise ValueError(msg)


def normalize(project: str) -> str:
    return re.sub(r"[-_.]+", "-", project).lower()


if __name__ == "__main__":
    main()
