CREATE TABLE backfill_window (
    run_id       INTEGER NOT NULL REFERENCES pipeline_run,
    wiki         TEXT NOT NULL REFERENCES wiki,
    window_from  TIMESTAMPTZ NOT NULL,
    window_to    TIMESTAMPTZ NOT NULL,
    changes_sent INTEGER NOT NULL,
    PRIMARY KEY (run_id, wiki, window_from),
    CHECK (window_to > window_from)
);