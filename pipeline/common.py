import csv
import json
import os
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Producer
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))
HEADERS = {"User-Agent": "african-wikipedia-live-activity/0.1 (https://github.com/Chichay317/african-wikipedia-live-activity)"}
TOPIC = "wiki.changes"
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def connect():
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD not found. Check that .env is in the project folder.")
    return psycopg.connect(host="localhost", port=int(os.getenv("POSTGRES_PORT", 5434)),
                           dbname="wiki_activity", user="wiki", password=password)


def load_wikis(conn, path="data/african_wikis.csv"):
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        conn.execute("""INSERT INTO wiki (wiki, code, language, url, closed) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (wiki) DO UPDATE SET code = EXCLUDED.code, language = EXCLUDED.language,
                        url = EXCLUDED.url, closed = EXCLUDED.closed""",
                     (r["wiki"], r["code"], r["language"], r["url"], r["closed"] == "True"))
    return {r["wiki"]: r["url"] for r in rows if r["closed"] != "True"}


# --- the common shape: a change looks the same whichever source it came from (checked in Step 1, Cell 6) ---

def none_if_zero(x):
    return None if x in (0, None) else x


def lengths(kind, old, new):
    if kind not in ("edit", "new"):
        return None, None
    return (None if kind == "new" else old), new


def from_feed(e):
    len_old, len_new = lengths(e["type"], (e.get("length") or {}).get("old"), (e.get("length") or {}).get("new"))
    return {"wiki": e["wiki"], "rc_id": e["id"], "type": e["type"], "namespace": e.get("namespace"),
            "title": e.get("title"), "user": e.get("user"), "bot": bool(e.get("bot")),
            "changed_at": datetime.fromtimestamp(e["timestamp"], timezone.utc).isoformat(),
            "rev_old": none_if_zero((e.get("revision") or {}).get("old")),
            "rev_new": none_if_zero((e.get("revision") or {}).get("new")),
            "len_old": len_old, "len_new": len_new}


def from_api(c, wiki):
    len_old, len_new = lengths(c["type"], c.get("oldlen"), c.get("newlen"))
    return {"wiki": wiki, "rc_id": c["rcid"], "type": c["type"], "namespace": c.get("ns"),
            "title": c.get("title"), "user": c.get("user"), "bot": bool(c.get("bot")),
            "changed_at": datetime.fromisoformat(c["timestamp"].replace("Z", "+00:00")).isoformat(),
            "rev_old": none_if_zero(c.get("old_revid")), "rev_new": none_if_zero(c.get("revid")),
            "len_old": len_old, "len_new": len_new}


# --- sending to Kafka ----------------------------------------------------------------------------------

class Delivery:
    def __init__(self):
        self.confirmed, self.failed = 0, 0

    def __call__(self, err, msg):
        if err is None:
            self.confirmed += 1
        else:
            self.failed += 1
            print(f"  Kafka did not accept a message: {err}")


def make_producer():
    return Producer({"bootstrap.servers": KAFKA,
                     "enable.idempotence": True,  
                     "acks": "all",               
                     "linger.ms": 50})


def send(producer, change, source, delivery):
    producer.produce(TOPIC, key=change["wiki"], value=json.dumps({**change, "source": source}),
                     on_delivery=delivery)


def confirm_all(producer, delivery, timeout=30):
    waiting = producer.flush(timeout)
    if waiting or delivery.failed:
        raise RuntimeError(f"Kafka has not confirmed {waiting} message(s) and rejected {delivery.failed}")