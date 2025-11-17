from __future__ import annotations

import logging
from typing import Iterable

import pytest
from pyspark.sql import SparkSession

RESET = "\033[0m"
COLOR_ERROR = "\033[31m"
COLOR_CODE = "\033[90m"
COLOR_TEST = "\033[33m"


def _enable_windows_ansi_colors() -> None:
    try:
        from colorama import just_fix_windows_console  # type: ignore
    except ImportError:  # pragma: no cover - optional dependency
        return
    just_fix_windows_console()


class ColoredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        color = ""
        if record.levelno >= logging.ERROR:
            color = COLOR_ERROR
        elif record.levelno == logging.DEBUG:
            color = COLOR_CODE
        if color:
            return f"{color}{message}{RESET}"
        return message


def _configure_module_logger() -> None:
    logger = logging.getLogger("src.inc_join")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    handler = logging.StreamHandler()
    handler.setFormatter(
        ColoredFormatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )

    _remove_existing_handlers(logger.handlers, ColoredFormatter)
    logger.addHandler(handler)


def _remove_existing_handlers(
    handlers: Iterable[logging.Handler], formatter_type: type[logging.Formatter]
) -> None:
    for handler in list(handlers):
        fmt = getattr(handler, "formatter", None)
        if isinstance(fmt, formatter_type):
            handler.close()
            handlers.remove(handler)


def pytest_configure(config: pytest.Config) -> None:  # noqa: D401 - pytest hook
    """Pytest hook to configure logging colour output."""
    _enable_windows_ansi_colors()
    _configure_module_logger()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None):
    terminal_reporter = item.config.pluginmanager.get_plugin("terminalreporter")
    if terminal_reporter is not None:
        terminal_reporter.write_line(f"{COLOR_TEST}{item.name}{RESET}")
    yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    outcome = yield
    report = outcome.get_result()
    if report.failed and hasattr(report, "longreprtext"):
        terminal_reporter = item.config.pluginmanager.get_plugin("terminalreporter")
        if terminal_reporter is None:
            return
        terminal_reporter.write_line("")
        for line in report.longreprtext.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("E"):
                terminal_reporter.write_line(f"{COLOR_ERROR}{line}{RESET}")
            elif stripped.startswith(">"):
                terminal_reporter.write_line(f"{COLOR_CODE}{line}{RESET}")
            else:
                terminal_reporter.write_line(line)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("inc_join_tests")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "Europe/Amsterdam")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    # Ensure timezone is set (redundant but ensures it's applied)
    session.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")
    yield session
    session.stop()
