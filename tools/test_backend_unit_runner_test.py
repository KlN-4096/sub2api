import importlib.util
import io
import pathlib
import re
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "unit_runner", pathlib.Path(__file__).with_name("test-backend-unit.py")
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class UnitBatchesTest(unittest.TestCase):
    def test_complete_unique_inventory_includes_examples_and_fuzz_seeds(self):
        names = [f"TestCase{index}" for index in range(8577)] + ["Example", "Example_demo", "FuzzInput"]
        listing = "\n".join(["initialization log", "BenchmarkIgnored", *reversed(names), ""])
        for shard_count in runner.SHARDED_PACKAGES.values():
            with self.subTest(shard_count=shard_count):
                batches = runner.unit_batches(listing, shard_count)
                flattened = [name for batch in batches for name in batch]
                self.assertCountEqual(names, flattened)
                self.assertEqual(len(set(flattened)), len(flattened))
                self.assertEqual(shard_count, len(batches))
                self.assertLessEqual(max(map(len, batches)) - min(map(len, batches)), 1)
                self.assertEqual(batches, runner.unit_batches("\n".join(names), shard_count))

    def test_small_inventory_has_no_empty_batches_and_exact_patterns(self):
        batches = runner.unit_batches("TestOne\nTestOneLonger\nExample_demo\n", 16)
        self.assertEqual(3, len(batches))
        for names in batches:
            pattern = "^(" + "|".join(map(re.escape, names)) + ")$"
            self.assertTrue(re.fullmatch(pattern, names[0]))
            self.assertFalse(re.fullmatch(pattern, names[0] + "Suffix"))

    def test_empty_or_duplicate_inventory_fails_closed(self):
        for listing in ("", "initialization log\n", "TestDuplicate\nTestDuplicate\n"):
            with self.subTest(listing=listing), self.assertRaises(ValueError):
                runner.unit_batches(listing, 16)

    def test_invalid_shard_count_fails(self):
        for shard_count in (0, -1):
            with self.subTest(shard_count=shard_count), self.assertRaises(ValueError):
                runner.unit_batches("TestOne\n", shard_count)

    def test_missing_or_duplicate_sharded_package_fails_before_execution(self):
        for name in runner.SHARDED_PACKAGES:
            for count in (0, 2):
                with self.subTest(package=name, count=count):
                    packages = [
                        f"example/internal/{candidate}"
                        for candidate in runner.SHARDED_PACKAGES
                        for _ in range(count if candidate == name else 1)
                    ]

                    def fake_run(args, capture=False, cwd=runner.BACKEND):
                        self.assertEqual(["go", "list"], args[:2])
                        if args[-1] == "./...":
                            return "\n".join(packages)
                        return "example/" + args[-1].removeprefix("./")

                    with mock.patch.object(runner, "run", side_effect=fake_run), self.assertRaises(ValueError):
                        runner.main()

    def test_main_keeps_other_packages_and_runs_each_sharded_package_batch(self):
        commands = []
        working_directories = []

        def fake_run(args, capture=False, cwd=runner.BACKEND):
            commands.append(args)
            working_directories.append(cwd)
            if args[:2] == ["go", "list"]:
                if args[-1] in ("./internal/service", "./internal/handler"):
                    return "example/" + args[-1].removeprefix("./") + "\n"
                return (
                    "example/internal/service\nexample/internal/handler\n"
                    "example/internal/service/child\nexample/internal/handler/admin\nexample/other\n"
                )
            if args[-1] == "-test.list=.":
                return "TestOne\nExample_demo\nFuzzInput\n"
            return None

        with mock.patch.object(runner, "run", side_effect=fake_run), mock.patch("sys.stdout", new=io.StringIO()):
            runner.main()
        self.assertIn(
            ["go", "test", "-timeout=60s", "-tags=unit", "example/internal/service/child",
             "example/internal/handler/admin", "example/other"],
            commands,
        )
        batches = [args for args in commands if "-test.timeout=60s" in args]
        self.assertEqual(6, len(batches))
        for args, cwd in zip(commands, working_directories):
            package_name = pathlib.Path(args[0]).name.split(".")[0]
            expected = runner.BACKEND if args[0] == "go" else runner.BACKEND / "internal" / package_name
            self.assertEqual(expected, cwd, "compiled tests must use the package directory, like go test")
        for package_name in ("service", "handler"):
            package_batches = [
                args for args in batches if pathlib.Path(args[0]).name.startswith(package_name + ".test")
            ]
            self.assertEqual(
                1, sum(args[:3] == ["go", "test", "-c"] and args[-1] == f"example/internal/{package_name}"
                       for args in commands),
            )
            for name in ("TestOne", "Example_demo", "FuzzInput"):
                self.assertEqual(
                    1, sum(bool(re.fullmatch(args[-1].removeprefix("-test.run="), name))
                           for args in package_batches)
                )


if __name__ == "__main__":
    unittest.main()
