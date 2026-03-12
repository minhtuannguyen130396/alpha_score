from datetime import datetime
from threading import current_thread

from settings import LOG_PREFIX


def _prefix(symbol: str | None = None) -> str:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    thread_name = current_thread().name
    parts = [stamp, LOG_PREFIX, thread_name]
    if symbol:
        parts.append(symbol)
    return " | ".join(parts)


def log_step(message: str, symbol: str | None = None) -> None:
    print(f"{_prefix(symbol)} | STEP | {message}")


def log_data(title: str, data, symbol: str | None = None) -> None:
    print(f"{_prefix(symbol)} | {title}: {data}")
