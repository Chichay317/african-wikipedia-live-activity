import argparse
import json
import time
from datetime import datetime, timedelta, timezone

import requests

from pipeline.common import HEADERS, Delivery, confirm_all, connect, from_api, load_wikis, make_producer, send

HISTORY_DAYS = 29              
SETTLE = timedelta(minutes=10)      
FEED_START_MARGIN = timedelta(minutes=1)
FEED_END_MARGIN = timedelta(minutes=5) 
MIN_GAP = timedelta(minutes=1)      

def api_get(url, params):
    problem = None
    for attempt in range(1, 6):
        try:
            r = requests.get(url + "/w/api.php", params=params, headers=HEADERS, timeout=60)
            time.sleep(0.2)
            if r.status_code != 429 and r.status_code < 500:
                r.raise_for_status()
                data = r.json()
                if "error" in data:
                    raise RuntimeError(f"API error: {data['error']}")
                return data
            problem = f"HTTP {r.status_code}"
        except (requests.Timeout, requests.ConnectionError) as err:
            problem = type(err).__name__
        if attempt < 5:
            wait = 15 * 2 ** (attempt - 1)
            print(f"  {problem} from {url}, waiting {wait} s ...")
            time.sleep(wait)
    raise RuntimeError(f"gave up on {url} after 5 attempts: {problem}")


def recent_changes(url, start, end):
    params = {"action": "query", "list": "recentchanges", "rcstart": f"{end:%Y-%m-%dT%H:%M:%SZ}",
              "rcend": f"{start:%Y-%m-%dT%H:%M:%SZ}", "rcprop": "ids|timestamp|flags|user|title|sizes",
              "rclimit": 500, "format": "json", "formatversion": 2}
    changes = []
    while True:
        data = api_get(url, params)
        changes += data["query"]["recentchanges"]
        if "continue" not in data:
            return changes
        params.update(data["continue"])


def gaps(window_from, window_to, covered):
    result, cursor = [], window_from
    for start, end in sorted(covered):
        if end <= cursor or start >= window_to:
            continue
        if start > cursor:
            result.append((cursor, min(start, window_to)))
        cursor = max(cursor, end)
        if cursor >= window_to:
            break
    if cursor < window_to:
        result.append((cursor, window_to))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=float, default=HISTORY_DAYS, help="how far back to look (at most 29)")
    args = parser.parse_args()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    window_from, window_to = now - timedelta(days=min(args.days, HISTORY_DAYS)), now - SETTLE

    conn = connect()
    conn.autocommit = True
    wikis = load_wikis(conn)
    run_id = conn.execute("INSERT INTO pipeline_run (job) VALUES ('backfill') RETURNING run_id").fetchone()[0]
    feed_covered = [(s + FEED_START_MARGIN, e - FEED_END_MARGIN) for s, e in conn.execute(
        "SELECT started_at, ended_at FROM feed_session WHERE ended_at IS NOT NULL").fetchall()
        if e - FEED_END_MARGIN > s + FEED_START_MARGIN]
    print(f"Run {run_id}: looking for gaps from {window_from:%Y-%m-%d %H:%M} to {window_to:%Y-%m-%d %H:%M} UTC; "
          f"{len(feed_covered)} feed session(s) count as covered")

    producer, delivery = make_producer(), Delivery()
    totals, failed, status = {"gaps": 0, "changes": 0}, [], "failed"
    try:
        for wiki, url in sorted(wikis.items()):
            done = conn.execute("SELECT window_from, window_to FROM backfill_window WHERE wiki = %s", (wiki,)).fetchall()
            todo = [(a, b) for a, b in gaps(window_from, window_to, feed_covered + done) if b - a >= MIN_GAP]
            sent = 0
            try:
                for g_from, g_to in todo:
                    changes = recent_changes(url, g_from, g_to)
                    for c in changes:
                        send(producer, from_api(c, wiki), "api", delivery)
                    confirm_all(producer, delivery)                   
                    conn.execute("INSERT INTO backfill_window (run_id, wiki, window_from, window_to, changes_sent) "
                                 "VALUES (%s, %s, %s, %s, %s)", (run_id, wiki, g_from, g_to, len(changes)))
                    sent += len(changes)
            except Exception as err:                              
                failed.append(wiki)
                print(f"{wiki:>9}: FAILED ({err}); its remaining gaps will be retried next run")
                continue
            totals["gaps"] += len(todo)
            totals["changes"] += sent
            if todo:
                print(f"{wiki:>9}: {len(todo)} gap(s), {sent} changes sent")
        status = "succeeded" if not failed else "failed"
    finally:
        conn.execute("UPDATE pipeline_run SET status = %s, finished_at = now(), note = %s WHERE run_id = %s",
                     (status, json.dumps({**totals, "failed": failed, "confirmed": delivery.confirmed}), run_id))
        conn.close()
        print(f"\nRun {run_id} {status}: {totals['gaps']} gaps filled, {totals['changes']} changes sent, "
              f"{delivery.confirmed} confirmed by Kafka" + (f"; failed: {failed}" if failed else ""))


if __name__ == "__main__":
    main()