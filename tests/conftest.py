"""Pytest plugin entrypoint for shared fixtures."""

pytest_plugins = [
    "tests.fixtures.cloud_fixtures",
    "tests.fixtures.runtime_fixtures",
    "tests.fixtures.common_fixtures",
]

