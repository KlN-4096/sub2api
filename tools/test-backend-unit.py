"""Run the complete unit suite with a 60-second limit per test process.

The service package has thousands of tests; compile it once and run every
listed test/example/fuzz seed in exactly one deterministic batch.
"""
import os
import pathlib
import re
import subprocess
import tempfile

BACKEND = pathlib.Path(__file__).resolve().parents[1] / "backend"
SERVICE_SHARDS = 64
TEST_TIMEOUT = "60s"
TEST_NAME = re.compile(r"^(?:Test|Example|Fuzz)\w*$")


def service_batches(listing):
    names = [line for line in listing.splitlines() if TEST_NAME.fullmatch(line)]
    if not names or len(names) != len(set(names)):
        raise ValueError("service test listing must be nonempty and unique")
    names.sort()
    return [names[index::SERVICE_SHARDS] for index in range(min(SERVICE_SHARDS, len(names)))]


def run(args, capture=False, cwd=BACKEND):
    return subprocess.run(
        args, cwd=cwd, check=True, text=True,
        stdout=subprocess.PIPE if capture else None,
    ).stdout


def main():
    service = run(["go", "list", "./internal/service"], capture=True).strip()
    packages = run(["go", "list", "-tags=unit", "./..."], capture=True).splitlines()
    if packages.count(service) != 1:
        raise ValueError("service package missing or duplicated in unit package list")
    other_packages = [package for package in packages if package != service]
    run(["go", "test", "-timeout=" + TEST_TIMEOUT, "-tags=unit", *other_packages])
    with tempfile.TemporaryDirectory(prefix="sub2api-unit-") as directory:
        binary = pathlib.Path(directory) / ("service.test.exe" if os.name == "nt" else "service.test")
        run(["go", "test", "-c", "-tags=unit", "-o", str(binary), service])
        package_directory = BACKEND / "internal" / "service"
        batches = service_batches(run([str(binary), "-test.list=."], capture=True, cwd=package_directory))
        total = sum(map(len, batches))
        print(f"Service unit inventory: {total} tests across {len(batches)} batches", flush=True)
        for index, names in enumerate(batches, 1):
            pattern = "^(" + "|".join(map(re.escape, names)) + ")$"
            print(f"Service unit batch {index}/{len(batches)}: {len(names)} tests", flush=True)
            run([str(binary), "-test.timeout=" + TEST_TIMEOUT, "-test.run=" + pattern], cwd=package_directory)


if __name__ == "__main__":
    main()
