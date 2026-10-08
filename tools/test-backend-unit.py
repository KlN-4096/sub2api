"""Run the complete unit suite with a 60-second limit per test process.

Compile large packages once and run every listed test/example/fuzz seed
in exactly one deterministic batch.
"""
import os
import pathlib
import re
import subprocess
import tempfile

BACKEND = pathlib.Path(__file__).resolve().parents[1] / "backend"
SHARDED_PACKAGES = {"service": 64, "handler": 16}
TEST_TIMEOUT = "60s"
TEST_NAME = re.compile(r"^(?:Test|Example|Fuzz)\w*$")


def unit_batches(listing, shard_count):
    if shard_count < 1:
        raise ValueError("shard count must be positive")
    names = [line for line in listing.splitlines() if TEST_NAME.fullmatch(line)]
    if not names or len(names) != len(set(names)):
        raise ValueError("test listing must be nonempty and unique")
    names.sort()
    return [names[index::shard_count] for index in range(min(shard_count, len(names)))]


def run(args, capture=False, cwd=BACKEND):
    return subprocess.run(
        args, cwd=cwd, check=True, text=True,
        stdout=subprocess.PIPE if capture else None,
    ).stdout


def run_sharded_package(name, package, shard_count):
    with tempfile.TemporaryDirectory(prefix="sub2api-unit-") as directory:
        binary = pathlib.Path(directory) / (name + (".test.exe" if os.name == "nt" else ".test"))
        run(["go", "test", "-c", "-tags=unit", "-o", str(binary), package])
        package_directory = BACKEND / "internal" / name
        batches = unit_batches(
            run([str(binary), "-test.list=."], capture=True, cwd=package_directory), shard_count
        )
        total = sum(map(len, batches))
        print(f"{name} unit inventory: {total} tests across {len(batches)} batches", flush=True)
        for index, names in enumerate(batches, 1):
            pattern = "^(" + "|".join(map(re.escape, names)) + ")$"
            print(f"{name} unit batch {index}/{len(batches)}: {len(names)} tests", flush=True)
            run([str(binary), "-test.timeout=" + TEST_TIMEOUT, "-test.run=" + pattern], cwd=package_directory)


def main():
    sharded = {
        name: run(["go", "list", "./internal/" + name], capture=True).strip()
        for name in SHARDED_PACKAGES
    }
    packages = run(["go", "list", "-tags=unit", "./..."], capture=True).splitlines()
    for name, package in sharded.items():
        if packages.count(package) != 1:
            raise ValueError(f"{name} package missing or duplicated in unit package list")
    other_packages = [package for package in packages if package not in sharded.values()]
    if other_packages:
        run(["go", "test", "-timeout=" + TEST_TIMEOUT, "-tags=unit", *other_packages])
    for name, package in sharded.items():
        run_sharded_package(name, package, SHARDED_PACKAGES[name])


if __name__ == "__main__":
    main()
