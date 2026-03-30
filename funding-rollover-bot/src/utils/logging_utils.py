"""Structured logging utilities using structlog."""
import logging
import sys
from typing import Optional

try:
    import structlog
    _HAS_STRUCTLOG = True
except ImportError:
    _HAS_STRUCTLOG = False


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Configure structured logging (structlog if available, else stdlib)."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=handlers,
    )

    if _HAS_STRUCTLOG:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer(),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )


def get_logger(name: str):
    """Return a structured logger bound to the given name."""
    if _HAS_STRUCTLOG:
        return structlog.get_logger(name)
    return _StdlibAdapter(logging.getLogger(name))


class _StdlibAdapter:
    """Minimal structlog-compatible wrapper around stdlib logger."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def _format(self, event: str, **kw) -> str:
        if kw:
            pairs = " ".join(f"{k}={v!r}" for k, v in kw.items())
            return f"{event} {pairs}"
        return event

    def debug(self, event: str, **kw) -> None:
        self._logger.debug(self._format(event, **kw))

    def info(self, event: str, **kw) -> None:
        self._logger.info(self._format(event, **kw))

    def warning(self, event: str, **kw) -> None:
        self._logger.warning(self._format(event, **kw))

    def error(self, event: str, **kw) -> None:
        self._logger.error(self._format(event, **kw))

    def critical(self, event: str, **kw) -> None:
        self._logger.critical(self._format(event, **kw))

    def exception(self, event: str, **kw) -> None:
        self._logger.exception(self._format(event, **kw))
