import argparse
import calendar
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from settings import BASE_DIR, DATA_ROOT

# ------ Cấu hình chung ------
BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoyMDUxMjk1NTI4LCJuYmYiOjE3NTEyOTU1MjgsImNsaWVudF9pZCI6ImZpcmVhbnQud2ViIiwic2NvcGUiOlsib3BlbmlkIiwicHJvZmlsZSIsInJvbGVzIiwiZW1haWwiLCJhY2NvdW50cy1yZWFkIiwiYWNjb3VudHMtd3JpdGUiLCJvcmRlcnMtcmVhZCIsIm9yZGVycy13cml0ZSIsImNvbXBhbmllcy1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImZpbmFuY2UtcmVhZCIsInBvc3RzLXdyaXRlIiwicG9zdHMtcmVhZCIsInN5bWJvbHMtcmVhZCIsInVzZXItZGF0YS1yZWFkIiwidXNlci1kYXRhLXdyaXRlIiwidXNlcnMtcmVhZCIsInNlYXJjaCIsImFjYWRlbXktcmVhZCIsImFjYWRlbXktd3JpdGUiLCJibG9nLXJlYWQiLCJpbnZlc3RvcGVkaWEtcmVhZCJdLCJzdWIiOiI4OWNmNTRiMy1kN2RkLTQyN2QtODI1NC0wZWU5MTE3ZDc2YjQiLCJhdXRoX3RpbWUiOjE3NTEyOTU1MjgsImlkcCI6Ikdvb2dsZSIsIm5hbWUiOiJtaW5odHVhbi5uZ3V5ZW4xMzAzOTZAZ21haWwuY29tIiwic2VjdXJpdHlfc3RhbXAiOiI2OTgyMDEzMy0xNTkwLTQwZDEtYjQyNC1hNDQ5Y2I3Mzk1OWIiLCJqdGkiOiI1OTlmZDA0ZmQ1OTVhMmU5YjBiYzhlZjhhNjY2YjY4NyIsImFtciI6WyJleHRlcm5hbCJdfQ.kjkBVCtjOD_4DEpSWVl7mwlCslG4-g275KaLBMTHfDhRumyNQx2wwkvWRH4DsWQWEpJohTHbxdlNRAXzCViGFg0tvAeOWyg408Ho27BGgD5CXYHLJFuPhq7sPP_TYplGQ5wnglxd35VfaVBllQr69hD7dC1omAPjBLLo93MqDU8n5_Z_yCPbFasKceMUncGOLPpI4sgEVTlM_XJsxgpp5qsX1RnxMeLJRHNJrSpqKPzf1Pw-5qL37EBqXlH_sp5sshS-_oyH1fFiNjYHnKu3bAzQal7oVW5jRD5qocjkU2a2hfjaJW0AcDmfDGafenv-mT7koCo-kbaWC_JCCxuPfQ"
BASE_URL_TEMPLATE = "https://restv2.fireant.vn/symbols/{share_code}/historical-quotes"
HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Accept": "application/json",
}
IS_WEEKLY_FETCH = True
DATE_START_FETCH = "2025-01-01"
LIST_VN_30 = BASE_DIR / "stock_vn_30.txt"
HISTORICAL_PRICE_DIRNAME = "historical_price"
REQUEST_TIMEOUT_SEC = 30
SCHEDULE_TIMES = ((9, 15), (15, 0))
# -----------------------------


def load_stocks_from_txt(path: Path = LIST_VN_30):
    with Path(path).open("r", encoding="utf-8") as handle:
        content = handle.read().strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        stocks = []
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [part.strip() for part in line.split(",")]
            if len(parts) >= 2:
                stocks.append({"share_code": parts[0], "ipo_date": parts[1]})
        return stocks


def end_of_month(value: date) -> date:
    last_day = calendar.monthrange(value.year, value.month)[1]
    return value.replace(day=last_day)


def first_day_of_next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def build_output_path(symbol: str, record_date: date) -> Path:
    year = str(record_date.year)
    return DATA_ROOT / symbol / HISTORICAL_PRICE_DIRNAME / year / f"{record_date.isoformat()}.json"


def fetch_symbol_data(symbol: str, start_date: date, end_date: date):
    days_count = (end_date - start_date).days + 1
    params = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "offset": 0,
        "limit": days_count,
    }

    url = BASE_URL_TEMPLATE.format(share_code=symbol)
    response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT_SEC)
    response.raise_for_status()
    return response.json()


def save_symbol_data(symbol: str, data) -> list[Path]:
    saved_paths: list[Path] = []

    records = data if isinstance(data, list) else [data]
    for record in records:
        record_date = datetime.strptime(record["date"], "%Y-%m-%dT%H:%M:%S").date()
        output_path = build_output_path(symbol, record_date)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, indent=2)
        saved_paths.append(output_path)

    return saved_paths


def fetch_all_data(run_date: date | None = None):
    stocks = load_stocks_from_txt()
    today = run_date or date.today()

    for stock in stocks:
        symbol = stock["share_code"]
        ipo = datetime.strptime(stock["ipo_date"], "%Y-%m-%d").date()
        current = datetime.strptime(DATE_START_FETCH, "%Y-%m-%d").date() if IS_WEEKLY_FETCH else ipo

        while current <= today:
            period_end = end_of_month(current) if IS_WEEKLY_FETCH else current
            end_date = min(period_end, today)

            if current > end_date:
                break

            print(f"Fetching {symbol}: {current} -> {end_date}")
            data = fetch_symbol_data(symbol, current, end_date)
            saved_paths = save_symbol_data(symbol, data)
            print(f"Saved {symbol} to {len(saved_paths)} daily files")

            if IS_WEEKLY_FETCH:
                current = first_day_of_next_month(current)
            else:
                current += timedelta(days=1)

    print("All data fetched successfully.")


def get_next_run_time(now: datetime | None = None) -> datetime:
    current = now or datetime.now()
    candidates = [
        current.replace(hour=hour, minute=minute, second=0, microsecond=0)
        for hour, minute in SCHEDULE_TIMES
    ]

    for candidate in candidates:
        if candidate > current:
            return candidate

    first_hour, first_minute = SCHEDULE_TIMES[0]
    next_day = current + timedelta(days=1)
    return next_day.replace(hour=first_hour, minute=first_minute, second=0, microsecond=0)


def run_scheduler():
    print("Historical price scheduler started.")
    print("Daily run times: 09:15 and 15:00")

    while True:
        next_run = get_next_run_time()
        wait_seconds = max(0.0, (next_run - datetime.now()).total_seconds())
        print(f"Next run at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(wait_seconds)

        started_at = datetime.now()
        print(f"Running fetch job at {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            fetch_all_data(run_date=started_at.date())
        except Exception as exc:
            print(f"Fetch job failed: {exc}")


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch historical price data from FireAnt.")
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run the fetch job immediately once, then exit.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.run_once:
        fetch_all_data()
        return

    run_scheduler()


if __name__ == "__main__":
    main()
