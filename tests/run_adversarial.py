"""Run the deterministic adversarial pack; --strict exposes known gaps as failures."""
import argparse
from pathlib import Path
import unittest


def cases(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from cases(item)
        else:
            yield item


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="Treat known expected failures as real failures")
    args = parser.parse_args()
    suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent), pattern="test_adversarial*.py")
    if args.strict:
        for case in cases(suite):
            method = getattr(type(case), case._testMethodName)
            if getattr(method, "__unittest_expecting_failure__", False):
                method.__unittest_expecting_failure__ = False
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
