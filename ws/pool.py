from concurrent.futures import ThreadPoolExecutor

from settings import MAX_WORKERS
from ws.session import run_symbol_worker


def run_pool(symbols, command_map) -> None:
    worker_count = min(MAX_WORKERS, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="symbol-worker") as executor:
        futures = [executor.submit(run_symbol_worker, symbol, command_map) for symbol in symbols]
        for future in futures:
            future.result()
