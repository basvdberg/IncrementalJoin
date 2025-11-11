import logging
import sys
from pathlib import Path

import pytest

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


class GreyDebugFormatter(logging.Formatter):
    """Custom formatter that colors DEBUG messages light grey."""

    # ANSI color codes
    LIGHT_GREY = "\033[2;37m"  # Dimmed light grey (softer, more subtle)
    RESET = "\033[0m"

    def format(self, record):
        # Format the message first using the parent formatter
        formatted_message = super().format(record)
        # Color DEBUG messages light grey by wrapping the entire formatted message
        if record.levelno == logging.DEBUG:
            return f"{self.LIGHT_GREY}{formatted_message}{self.RESET}"
        return formatted_message


class YellowInfoFormatter(logging.Formatter):
    """Custom formatter that colors INFO messages yellow."""

    YELLOW = "\033[33m"
    RESET = "\033[0m"

    def format(self, record):
        formatted_message = super().format(record)
        if record.levelno == logging.INFO:
            return f"{self.YELLOW}{formatted_message}{self.RESET}"
        return formatted_message


# Configure logging for tests
# Set root logger to WARNING to suppress noisy DEBUG logs from other modules
root_logger = logging.getLogger()
root_logger.setLevel(logging.WARNING)

# Create a console handler with the custom formatter
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(
    GreyDebugFormatter(
        fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)

# Add handler to root logger if it doesn't already have one
if not root_logger.handlers:
    root_logger.addHandler(console_handler)

# Enable debug logging ONLY for inc_join module
# The module name is 'src.inc_join' when imported via the src package
inc_join_logger = logging.getLogger("src.inc_join")
inc_join_logger.setLevel(logging.DEBUG)
# Create a separate handler for DEBUG messages with the grey formatter
debug_handler = logging.StreamHandler()
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(
    GreyDebugFormatter(
        fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)
inc_join_logger.addHandler(debug_handler)
inc_join_logger.propagate = (
    False  # Prevent propagation to root logger to avoid duplicate messages
)

# Enable INFO logging for test module to show connection messages
logging.getLogger("tests.test_inc_join").setLevel(logging.INFO)
logging.getLogger("tests").setLevel(logging.INFO)

# Create a handler for test INFO logs with yellow coloring
tests_info_handler = logging.StreamHandler()
tests_info_handler.setLevel(logging.INFO)
tests_info_handler.setFormatter(
    YellowInfoFormatter(
        fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)
tests_logger = logging.getLogger("tests")
if not any(
    isinstance(handler.formatter, YellowInfoFormatter)
    for handler in tests_logger.handlers
    if handler.formatter
):
    tests_logger.addHandler(tests_info_handler)
tests_logger.propagate = False

# Suppress DEBUG logs from noisy third-party loggers
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)


@pytest.fixture(autouse=True)
def log_test_name(request):
    logger = logging.getLogger("tests")
    logger.info("Running test: %s", request.node.name)
    yield
