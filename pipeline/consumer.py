import argparse
import json
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaException

from pipeline.common import KAFKA, TOPIC, connect

GROUP = "wiki-store"
BATCH = 500
TYPES = {"edit", "new", "log", "categorize", "external"}
COLUMNS = ["wiki", "rc_id", "type", "namespace", "title", "user_name", "bot", "changed_at",
           "rev_old", "rev_new", "len_old", "len_new", "source", "kafka_partition", "kafka_offset"]

INSERT_SQL = f"""
WITH stored AS (
    INSERT INTO change ({", ".join(COLUMNS)})
    SELECT {", ".join(COLUMNS)} FROM incoming
    ON CONFLICT (wiki, rc_id) DO NOTHING            -- already stored: it becomes a counted duplicate
    RETURNING 1
)
SELECT count(*) FROM stored
"""


def check(value, wikis):
    try:
        m = json.loads(value)
    except (TypeError, ValueError) as err:
        return None, f"not valid JSON: {err}"
    if not isinstance(m, dict):
        return None, "not a JSON object"
    if m.get("wiki") not in wikis:
        return None, f"unknown wiki {m.get('wiki')!r}"
    if not isinstance(m.get("rc_id"), int) or m["rc_id"] <= 0:
        return None, f"bad rc_id {m.get('rc_id')!r}"
    if m.get("type") not in TYPES:
        return None, f"unknown type {m.get('type')!r}"
    if m.get("source") not in ("feed", "api"):
        return None, f"unknown source {m.get('source')!r}"
    try:
        changed_at = datetime.fromisoformat(m["changed_at"])
    except (KeyError, TypeError, ValueError):
        return None, f"bad changed_at {m.get('changed_at')!r}"
    if changed_at.tzinfo is None:
        return None, "changed_at has no time zone"
    return {"wiki": m["wiki"], "rc_id": m["rc_id"], "type": m["type"], "namespace": m.get("namespace"),
            "title": m.get("title"), "user_name": m.get("user"), "bot": bool(m.get("bot")),
            "changed_at": changed_at, "rev_old": m.get("rev_old"), "rev_new": m.get("rev_new"),
            "len_old": m.get("len_old"), "len_new": m.get("len_new"), "source": m["source"]}, None


def store_batch(conn, run_id, batch_no, messages, wikis):
    rows, dead = [], []
    for msg in messages:
        row, error = check(msg.value(), wikis)
        if error:
            dead.append((msg.topic(), msg.partition(), msg.offset(), error,
                         (msg.value() or b"").decode("utf-8", errors="replace")))
        else:
            rows.append({**row, "kafka_partition": msg.partition(), "kafka_offset": msg.offset()})
    with conn.transaction():
        conn.execute("TRUNCATE incoming")
        with conn.cursor().copy(f"COPY incoming ({', '.join(COLUMNS)}) FROM STDIN") as cp:
            for r in rows:
                cp.write_row([r[c] for c in COLUMNS])
        inserted = conn.execute(INSERT_SQL).fetchone()[0]
        for d in dead:
            conn.execute("INSERT INTO dead_letter (kafka_topic, kafka_partition, kafka_offset, error, payload) "
                         "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", d)
        conn.execute("INSERT INTO consume_log (run_id, batch_no, messages, inserted, duplicates, dead_letters) "
                     "VALUES (%s, %s, %s, %s, %s, %s)",
                     (run_id, batch_no, len(messages), inserted, len(rows) - inserted, len(dead)))
    return len(messages), inserted, len(rows) - inserted, len(dead)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--follow", action="store_true", help="keep waiting for new messages (default: stop when idle)")
    parser.add_argument("--idle-seconds", type=float, default=15, help="stop after this long with no messages")
    args = parser.parse_args()

    conn = connect()
    conn.autocommit = True
    wikis = {w for (w,) in conn.execute("SELECT wiki FROM wiki").fetchall()}
    conn.execute("CREATE TEMP TABLE incoming (LIKE change INCLUDING DEFAULTS)")
    run_id = conn.execute("INSERT INTO pipeline_run (job) VALUES ('consumer') RETURNING run_id").fetchone()[0]
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": GROUP,
                         "enable.auto.commit": False,       
                         "auto.offset.reset": "earliest"})   
    consumer.subscribe([TOPIC])
    print(f"Run {run_id}: consuming {TOPIC} as group '{GROUP}'" + (" (follow mode, Ctrl+C to stop)" if args.follow else ""))

    totals = dict.fromkeys(["messages", "inserted", "duplicates", "dead_letters"], 0)
    status, batch_no, last_message = "failed", 0, time.time()
    try:
        while True:
            messages = consumer.consume(num_messages=BATCH, timeout=1.0)
            for msg in messages:
                if msg.error():
                    raise KafkaException(msg.error())
            if not messages:
                if not args.follow and time.time() - last_message > args.idle_seconds:
                    break
                continue
            last_message = time.time()
            batch_no += 1
            counts = store_batch(conn, run_id, batch_no, messages, wikis)  
            consumer.commit(asynchronous=False)                        
            totals = {k: totals[k] + c for k, c in zip(totals, counts)}
            if batch_no % 20 == 0 or len(messages) < BATCH:
                print(f"{time.strftime('%H:%M:%S')}  batch {batch_no}: " + ", ".join(f"{v:,} {k}" for k, v in totals.items()))
        status = "succeeded"
    except KeyboardInterrupt:
        print("\nStopped by you (Ctrl+C).")
        status = "succeeded"
    finally:
        consumer.close()
        conn.execute("UPDATE pipeline_run SET status = %s, finished_at = now(), note = %s WHERE run_id = %s",
                     (status, json.dumps(totals), run_id))
        conn.close()
        print(f"Run {run_id} {status}: " + ", ".join(f"{v:,} {k}" for k, v in totals.items()))


if __name__ == "__main__":
    main()