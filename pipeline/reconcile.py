import argparse
import json
import random
from datetime import datetime, timedelta, timezone

from pipeline.backfill import recent_changes
from pipeline.common import Delivery, confirm_all, connect, from_api, load_wikis, make_producer, send

API_MEMORY = timedelta(days=29)


def compare(conn, run_id, wiki, url, start, end, kind, repair, producer, delivery):
    api = {c["rcid"]: c for c in recent_changes(url, start, end)}
    stored = {r for (r,) in conn.execute("SELECT rc_id FROM change WHERE wiki = %s AND changed_at BETWEEN %s AND %s",
                                         (wiki, start, end)).fetchall()}
    missing, extra = set(api) - stored, stored - set(api)
    repaired = 0
    if repair and missing:
        for rc_id in missing:
            send(producer, from_api(api[rc_id], wiki), "api", delivery)
        confirm_all(producer, delivery)
        repaired = len(missing)
    conn.execute("INSERT INTO reconcile_result (run_id, wiki, window_from, window_to, kind, api_count, stored_count, "
                 "missing, extra, repaired) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                 (run_id, wiki, start, end, kind, len(api), len(stored), len(missing), len(extra), repaired))
    return {"api": len(api), "stored": len(stored), "missing": len(missing), "extra": len(extra),
            "repaired": repaired, "examples": sorted(missing)[:3] + sorted(extra)[:3]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=60, help="how many Wikipedia-hours to sample")
    parser.add_argument("--seed", type=int, default=1, help="makes the sample repeatable")
    parser.add_argument("--repair", action="store_true", help="re-send missing changes to Kafka")
    args = parser.parse_args()
    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    oldest_allowed = now - API_MEMORY

    conn = connect()
    conn.autocommit = True
    wikis = load_wikis(conn)
    run_id = conn.execute("INSERT INTO pipeline_run (job) VALUES ('reconcile') RETURNING run_id").fetchone()[0]

    windows = [(w, s, e, "feed_session") for s, e in conn.execute(
        "SELECT started_at, ended_at FROM feed_session WHERE ended_at - started_at >= interval '2 minutes' "
        "AND started_at > %s ORDER BY started_at", (oldest_allowed,)).fetchall() for w in sorted(wikis)]
    busy = conn.execute("SELECT wiki, date_trunc('hour', changed_at) FROM change WHERE changed_at > %s "
                        "AND changed_at < %s", (oldest_allowed, now - timedelta(hours=1))).fetchall()
    picked = rng.sample(busy, min(len(busy), args.samples // 2))
    hours = int((now - timedelta(hours=1) - oldest_allowed).total_seconds() // 3600)
    picked += [(rng.choice(sorted(wikis)), now - timedelta(hours=rng.randint(2, hours))) for _ in range(args.samples - len(picked))]
    windows += [(w, h, h + timedelta(minutes=59, seconds=59), "sample") for w, h in dict.fromkeys(picked)]
    print(f"Run {run_id}: checking {len(windows)} windows against Wikipedia's own record"
          + (" (repair on)" if args.repair else ""))

    producer, delivery = make_producer(), Delivery()
    totals, status = {}, "failed"
    try:
        for wiki, start, end, kind in windows:
            r = compare(conn, run_id, wiki, wikis[wiki], start, end, kind, args.repair, producer, delivery)
            t = totals.setdefault(kind, dict.fromkeys(["windows", "api", "stored", "missing", "extra", "repaired"], 0))
            t["windows"] += 1
            for k in ["api", "stored", "missing", "extra", "repaired"]:
                t[k] += r[k]
            if r["missing"] or r["extra"]:
                print(f"  {kind:12} {wiki:>9} {start:%Y-%m-%d %H:%M}: Wikipedia {r['api']}, stored {r['stored']}, "
                      f"missing {r['missing']}, extra {r['extra']} (e.g. rc_id {r['examples']})")
        status = "succeeded"
    finally:
        conn.execute("UPDATE pipeline_run SET status = %s, finished_at = now(), note = %s WHERE run_id = %s",
                     (status, json.dumps(totals), run_id))
        conn.close()
    print()
    for kind, t in totals.items():
        print(f"{kind:12}: {t['windows']} windows | Wikipedia lists {t['api']:,} | we stored {t['stored']:,} | "
              f"missing {t['missing']} | extra {t['extra']} | repaired {t['repaired']}")
    print(f"Run {run_id} {status}.")


if __name__ == "__main__":
    main()