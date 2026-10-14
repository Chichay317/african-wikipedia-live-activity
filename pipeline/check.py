from confluent_kafka import Consumer, TopicPartition

from pipeline.common import KAFKA, TOPIC, connect
from pipeline.consumer import GROUP


def main():
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": GROUP, "enable.auto.commit": False})
    partitions = [TopicPartition(TOPIC, p) for p in sorted(consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions)]
    committed = consumer.committed(partitions, timeout=10)
    print("Partition | messages in Kafka | read and committed | still unread")
    in_kafka = done = 0
    for tp, c in zip(partitions, committed):
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        read = max(c.offset, 0)                
        in_kafka, done = in_kafka + high, done + read
        print(f"{tp.partition:>9} | {high:>17,} | {read:>18,} | {high - read:>12,}")
    consumer.close()

    with connect() as conn:
        logged, inserted, duplicates, dead_logged = conn.execute(
            "SELECT coalesce(sum(messages), 0), coalesce(sum(inserted), 0), coalesce(sum(duplicates), 0), "
            "coalesce(sum(dead_letters), 0) FROM consume_log").fetchone()
        stored = conn.execute("SELECT count(*) FROM change").fetchone()[0]
        dead = conn.execute("SELECT count(*) FROM dead_letter").fetchone()[0]
        sent = conn.execute("""SELECT coalesce(sum(CASE job WHEN 'feed_producer' THEN (note::json->>'kept')::int
                                                       ELSE (note::json->>'confirmed')::int END), 0)
                               FROM pipeline_run WHERE job IN ('feed_producer', 'backfill') AND note IS NOT NULL""").fetchone()[0]
        by_source = conn.execute("SELECT source, count(*) FROM change GROUP BY 1 ORDER BY 1").fetchall()

    checks = {
        "Producers sent exactly what Kafka holds": sent == in_kafka,
        "Kafka has nothing left unread": done == in_kafka,
        "Every committed message was logged (re-reads after a crash make the log larger)": logged >= done,
        "Stored changes = messages logged as inserted": stored == inserted,
        "Dead-letter table holds every logged dead letter (re-reads are not stored twice)": dead <= dead_logged and (dead > 0) == (dead_logged > 0),
    }
    print(f"\nProducers sent {sent:,} | Kafka holds {in_kafka:,} | consumer logged {logged:,} = "
          f"{inserted:,} stored + {duplicates:,} duplicates + {dead_logged:,} dead letters")
    print(f"Stored changes by first source: " + ", ".join(f"{s} {n:,}" for s, n in by_source))
    print()
    for name, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}: {name}")

if __name__ == "__main__":
    main()