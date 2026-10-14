import json
from datetime import datetime, timezone

import pipeline.feed_producer as feed_producer
from pipeline.backfill import gaps
from pipeline.common import from_api, from_feed
from pipeline.consumer import check

FEED_EVENT = {"wiki": "swwiki", "type": "edit", "id": 5173384, "namespace": 4, "title": "Wikipedia:Mradi wa Nchi",
              "user": "Gayle-Bot", "bot": True, "timestamp": 1791851063,
              "length": {"old": 21885, "new": 21884}, "revision": {"old": 1607145, "new": 1607214}}
API_CHANGE = {"type": "edit", "ns": 4, "title": "Wikipedia:Mradi wa Nchi", "rcid": 5173384, "revid": 1607214,
              "old_revid": 1607145, "user": "Gayle-Bot", "bot": True, "oldlen": 21885, "newlen": 21884,
              "timestamp": "2026-10-13T00:24:23Z"}
WIKIS = {"swwiki", "yowiki"}


def t(h, m=0):
    return datetime(2026, 10, 1, h, m, tzinfo=timezone.utc)


def test_same_change_from_both_sources_is_identical():
    assert from_feed(FEED_EVENT) == from_api(API_CHANGE, "swwiki")

def test_new_page_has_no_before_size_or_revision_from_either_source():
    feed = {**FEED_EVENT, "type": "new", "length": {"new": 500}, "revision": {"new": 9}}
    api = {**API_CHANGE, "type": "new", "oldlen": 0, "old_revid": 0, "newlen": 500, "revid": 9}
    assert from_feed(feed) == from_api(api, "swwiki")
    assert from_feed(feed)["len_old"] is None and from_feed(feed)["rev_old"] is None

def test_log_entries_have_no_sizes():
    api = {**API_CHANGE, "type": "log", "oldlen": 0, "newlen": 0, "revid": 0, "old_revid": 0}
    row = from_api(api, "swwiki")
    assert (row["len_old"], row["len_new"], row["rev_old"], row["rev_new"]) == (None, None, None, None)


def test_nothing_covered_means_one_big_gap():
    assert gaps(t(0), t(10), []) == [(t(0), t(10))]

def test_overlapping_and_overhanging_coverage():
    assert gaps(t(0), t(10), [(t(2), t(3)), (t(2), t(5)), (t(7), t(12))]) == [(t(0), t(2)), (t(5), t(7))]

def test_fully_covered_means_no_gaps():
    assert gaps(t(0), t(10), [(t(0), t(4)), (t(4), t(10))]) == []

def test_coverage_outside_the_window_is_ignored():
    assert gaps(t(5), t(10), [(t(1), t(3))]) == [(t(5), t(10))]


def message(**changes):
    m = {**from_feed(FEED_EVENT), "source": "feed"}
    m.update(changes)
    return json.dumps(m).encode()

def test_good_message_becomes_a_row():
    row, error = check(message(), WIKIS)
    assert error is None and row["rc_id"] == 5173384 and row["user_name"] == "Gayle-Bot"

def test_bad_messages_are_rejected_with_a_reason():
    cases = {b"{not json": "not valid JSON", message(wiki="enwiki"): "unknown wiki", message(rc_id=-3): "bad rc_id",
             message(rc_id="12"): "bad rc_id", message(type="vandalism"): "unknown type", message(source="rumour"): "unknown source",
             message(changed_at="yesterday"): "bad changed_at", message(changed_at="2026-10-13T00:24:23"): "no time zone"}
    for value, reason in cases.items():
        row, error = check(value, WIKIS)
        assert row is None and reason in error, (value, error)


class FakeResponse:
    def __init__(self, lines):
        self.lines = lines
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def raise_for_status(self):
        pass
    def iter_lines(self):
        yield from (line.encode() for line in self.lines)

def test_feed_lines_become_events_with_their_ids(monkeypatch):
    lines = ["event: message", 'id: [{"offset":1}]', 'data: {"a": 1}', "",
             ":ok", "",                                  
             "event: message", 'id: [{"offset":2}]', 'data: {"a": 2}', ""]
    monkeypatch.setattr(feed_producer.requests, "get", lambda *a, **k: FakeResponse(lines))
    assert list(feed_producer.feed()) == [('[{"offset":1}]', '{"a": 1}'), ('[{"offset":2}]', '{"a": 2}')]

def test_resuming_sends_the_last_event_id(monkeypatch):
    sent = {}
    def fake_get(url, headers=None, **kwargs):
        sent.update(headers)
        return FakeResponse([])
    monkeypatch.setattr(feed_producer.requests, "get", fake_get)
    list(feed_producer.feed('[{"offset":41}]'))
    assert sent["Last-Event-ID"] == '[{"offset":41}]' and "User-Agent" in sent