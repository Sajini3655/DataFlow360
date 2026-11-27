import csv
import json
from pathlib import Path
from urllib.request import urlopen


def csv_adapter(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def json_adapter(path):
    with Path(path).open(encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, list) else payload.get("records", [])


def rest_adapter(url, timeout=15):
    with urlopen(url, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, list) else payload.get("records", [])


def postgres_adapter(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()
