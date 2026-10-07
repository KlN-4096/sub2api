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


class ServiceBatchesTest(unittest.TestCase):
    def test_complete_unique_inventory_includes_examples_and_fuzz_seeds(self):
        names = [f"TestCase{index}" for index in range(8577)] + ["Example", "Example_demo", "FuzzInput"]
        listing = "\n".join(["initialization log", "BenchmarkIgnored", *reversed(names), ""])
        batches = runner.service_batches(listing)
        flattened = [name for batch in batches for name in batch]
        self.assertCountEqual(names, flattened)
        self.assertEqual(len(set(flattened)), len(flattened))
        self.assertEqual(runner.SERVICE_SHARDS, len(batches))
        self.assertLessEqual(max(map(len, batches)) - min(map(len, batches)), 1)
        self.assertEqual(batches, runner.service_batches("\n".join(names)))

    def test_small_inventory_has_no_empty_batches_and_exact_patterns(self):
        batches = runner.service_batches("TestOne\nTestOneLonger\nExample_demo\n")
        self.assertEqual(3, len(batches))
        for names in batches:
            pattern = "^(" + "|".join(map(re.escape, names)) + ")$"
            self.assertTrue(re.fullmatch(pattern, names[0]))
            self.assertFalse(re.fullmatch(pattern, names[0] + "Suffix"))

    def test_empty_or_duplicate_inventory_fails_closed(self):
        for listing in ("", "initialization log\n", "TestDuplicate\nTestDuplicate\n"):
            with self.subTest(listing=listing), self.assertRaises(ValueError):
                runner.service_batches(listing)

    def test_main_keeps_other_packages_and_runs_each_service_batch(self):
        commands = []
        working_directories = []

        def fake_run(args, capture=False, cwd=runner.BACKEND):
            commands.append(args)
            working_directories.append(cwd)
            if args[:2] == ["go", "list"]:
                return "example/internal/service\n" if "./internal/service" in args else (
                    "example/internal/service\nexample/internal/service/child\nexample/other\n"
                )
            if args[-1] == "-test.list=.":
                return "TestOne\nExample_demo\nFuzzInput\n"
            return None

        with mock.patch.object(runner, "run", side_effect=fake_run), mock.patch("sys.stdout", new=io.StringIO()):
            runner.main()
        self.assertIn(
            ["go", "test", "-timeout=60s", "-tags=unit", "example/internal/service/child", "example/other"],
            commands,
        )
        batches = [args for args in commands if "-test.timeout=60s" in args]
        self.assertEqual(3, len(batches))
        for args, cwd in zip(commands, working_directories):
            expected = runner.BACKEND if args[0] == "go" else runner.BACKEND / "internal" / "service"
            self.assertEqual(expected, cwd, "compiled tests must use the package directory, like go test")
        for name in ("TestOne", "Example_demo", "FuzzInput"):
            self.assertEqual(
                1, sum(bool(re.fullmatch(args[-1].removeprefix("-test.run="), name)) for args in batches)
            )


if __name__ == "__main__":
    unittest.main()
