import argparse
import json
import time

import requests

from pipeline.common import HEADERS, Delivery, confirm_all, connect, from_feed, load_wikis, make_producer, send

STREAM = "https://stream.wikimedia.org/v2/stream/recentchange"
CHECKPOINT_SECONDS = 30
MAX_RECONNECTS = 5


def feed(last_id=None):
    headers = dict(HEADERS, **({"Last-Event-ID": last_id} if last_id else {}))
    with requests.get(STREAM, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        event_id, data = None, []
        for raw in r.iter_lines():
            line = raw.decode("utf-8", errors="replace")
            if line == "":                            
                if data:
                    yield event_id, "\n".join(data)
                event_id, data = None, []
            elif line.startswith("id:"):
                event_id = line[3:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].strip())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, help="stop after this many minutes (default: run until Ctrl+C)")
    args = parser.parse_args()
    deadline = time.time() + args.minutes * 60 if args.minutes else None

    conn = connect()
    conn.autocommit = True
    african = load_wikis(conn)
    run_id = conn.execute("INSERT INTO pipeline_run (job) VALUES ('feed_producer') RETURNING run_id").fetchone()[0]
    producer, delivery = make_producer(), Delivery()
    stats = dict.fromkeys(["seen", "kept", "canary", "malformed", "reconnects"], 0)
    session_id, last_id, covered_to, last_checkpoint = None, None, None, time.time()
    print(f"Run {run_id}: following {len(african)} Wikipedias" + (f" for {args.minutes:g} minutes" if deadline else " until Ctrl+C"))

    def checkpoint():
        confirm_all(producer, delivery)
        if session_id is not None:
            conn.execute("UPDATE feed_session SET ended_at = to_timestamp(%s), last_event_id = %s WHERE session_id = %s",
                         (covered_to, last_id, session_id))
        print(f"{time.strftime('%H:%M:%S')}  seen {stats['seen']:,} | kept {stats['kept']} | "
              f"confirmed by Kafka {delivery.confirmed} | reconnects {stats['reconnects']}")

    status, attempt, stop = "failed", 0, False
    try:
        while not stop:
            try:
                for event_id, data in feed(last_id):
                    attempt = 0
                    stats["seen"] += 1
                    last_id = event_id or last_id
                    try:
                        e = json.loads(data)
                    except json.JSONDecodeError:
                        stats["malformed"] += 1
                        continue
                    if e.get("meta", {}).get("domain") == "canary":
                        stats["canary"] += 1
                        continue
                    ts = e.get("timestamp")
                    if session_id is None and ts:
                        session_id = conn.execute("INSERT INTO feed_session (run_id, started_at) "
                                                  "VALUES (%s, to_timestamp(%s)) RETURNING session_id", (run_id, ts)).fetchone()[0]
                    if e.get("wiki") in african:
                        send(producer, from_feed(e), "feed", delivery)
                        stats["kept"] += 1
                    producer.poll(0)                
                    if ts:
                        covered_to = max(covered_to or ts, ts)
                    if time.time() - last_checkpoint >= CHECKPOINT_SECONDS:
                        checkpoint()
                        last_checkpoint = time.time()
                    if deadline and time.time() >= deadline:
                        stop = True
                        break
                else:                            
                    stats["reconnects"] += 1
            except requests.RequestException as err:       
                attempt += 1
                if attempt > MAX_RECONNECTS:
                    raise
                stats["reconnects"] += 1
                wait = 15 * 2 ** (attempt - 1)
                print(f"  Feed connection problem ({type(err).__name__}); resuming from the last event in {wait} s ...")
                time.sleep(wait)
        status = "succeeded"
    except KeyboardInterrupt:
        print("\nStopped by you (Ctrl+C).")
        status = "succeeded"
    finally:
        try:
            checkpoint()                         
        except Exception as err:
            print(f"Final checkpoint failed: {err}")
            status = "failed"
        conn.execute("UPDATE pipeline_run SET status = %s, finished_at = now(), note = %s WHERE run_id = %s",
                     (status, json.dumps(stats), run_id))
        conn.close()
        print(f"Run {run_id} {status}: " + ", ".join(f"{v:,} {k}" for k, v in stats.items()))


if __name__ == "__main__":
    main()