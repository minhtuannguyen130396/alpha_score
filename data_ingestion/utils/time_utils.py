from datetime import datetime


def now_parts():
    now = datetime.now()
    return now, str(now.year), now.strftime("%Y-%m-%d"), now.isoformat()
