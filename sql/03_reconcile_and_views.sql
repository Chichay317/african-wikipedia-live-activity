CREATE TABLE reconcile_result (
    run_id       INTEGER NOT NULL REFERENCES pipeline_run,
    wiki         TEXT NOT NULL REFERENCES wiki,
    window_from  TIMESTAMPTZ NOT NULL,
    window_to    TIMESTAMPTZ NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('feed_session', 'sample')),
    api_count    INTEGER NOT NULL,            
    stored_count INTEGER NOT NULL,                
    missing      INTEGER NOT NULL,                
    extra        INTEGER NOT NULL,                
    repaired     INTEGER NOT NULL DEFAULT 0,       
    PRIMARY KEY (run_id, wiki, window_from, kind)
);

CREATE OR REPLACE VIEW daily_activity AS
SELECT wiki,
       (changed_at AT TIME ZONE 'UTC')::date                                         AS day,
       count(*) FILTER (WHERE type IN ('edit', 'new'))                                AS actions,
       count(*) FILTER (WHERE type IN ('edit', 'new') AND NOT bot)                    AS human_actions,
       count(*) FILTER (WHERE type = 'new')                                           AS new_pages,
       count(*) FILTER (WHERE type = 'new' AND NOT bot)                               AS human_new_pages,
       count(DISTINCT user_name) FILTER (WHERE type IN ('edit', 'new') AND NOT bot)   AS human_editors,
       count(*) FILTER (WHERE type = 'log')                                           AS log_entries,
       count(*) FILTER (WHERE type = 'categorize')                                    AS categorisations
FROM change
GROUP BY 1, 2;

CREATE OR REPLACE VIEW wiki_summary AS
WITH recent AS (
    SELECT * FROM change
    WHERE type IN ('edit', 'new')
      AND changed_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' - interval '28 days'
      AND changed_at <  date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
),
top_editor AS (
    SELECT DISTINCT ON (wiki) wiki, count(*) AS top_actions
    FROM recent WHERE NOT bot GROUP BY wiki, user_name ORDER BY wiki, count(*) DESC
)
SELECT w.language, w.wiki,
       count(r.rc_id)                                                         AS actions,
       count(r.rc_id) FILTER (WHERE NOT r.bot)                                AS human_actions,
       round(100.0 * count(r.rc_id) FILTER (WHERE r.bot) / nullif(count(r.rc_id), 0))         AS bot_pct,
       count(DISTINCT r.user_name) FILTER (WHERE NOT r.bot)                   AS human_editors,
       count(r.rc_id) FILTER (WHERE r.type = 'new' AND NOT r.bot)             AS human_new_pages,
       count(DISTINCT (r.changed_at AT TIME ZONE 'UTC')::date) FILTER (WHERE NOT r.bot)       AS days_with_human_activity,
       round(100.0 * t.top_actions / nullif(count(r.rc_id) FILTER (WHERE NOT r.bot), 0))     AS top_editor_pct
FROM wiki w
LEFT JOIN recent r USING (wiki)
LEFT JOIN top_editor t USING (wiki)
WHERE NOT w.closed
GROUP BY w.language, w.wiki, t.top_actions;