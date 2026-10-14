CREATE TABLE wiki (                                
    wiki        TEXT PRIMARY KEY,                  
    code        TEXT NOT NULL,                      
    language    TEXT NOT NULL,                       
    url         TEXT NOT NULL,
    closed      BOOLEAN NOT NULL
);

CREATE TABLE pipeline_run (                      
    run_id      SERIAL PRIMARY KEY,
    job         TEXT NOT NULL,                     
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status      TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'succeeded', 'failed')),
    note        TEXT
);

CREATE TABLE change (                              
    wiki            TEXT NOT NULL REFERENCES wiki,
    rc_id           BIGINT NOT NULL,                
    type            TEXT NOT NULL CHECK (type IN ('edit', 'new', 'log', 'categorize', 'external')),
    namespace       INTEGER,
    title           TEXT,
    user_name       TEXT,
    bot             BOOLEAN NOT NULL,
    changed_at      TIMESTAMPTZ NOT NULL,            
    rev_old         BIGINT,
    rev_new         BIGINT,
    len_old         INTEGER,
    len_new         INTEGER,
    source          TEXT NOT NULL CHECK (source IN ('feed', 'api')),  
    kafka_partition INTEGER NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    stored_at       TIMESTAMPTZ NOT NULL DEFAULT now(),             
    PRIMARY KEY (wiki, rc_id)                   
);

CREATE TABLE feed_session (                      
    session_id    SERIAL PRIMARY KEY,             
    run_id        INTEGER NOT NULL REFERENCES pipeline_run,
    started_at    TIMESTAMPTZ NOT NULL,
    ended_at      TIMESTAMPTZ,                      
    last_event_id TEXT,                             
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE TABLE consume_log (                       
    run_id        INTEGER NOT NULL REFERENCES pipeline_run,
    batch_no      INTEGER NOT NULL,
    messages      INTEGER NOT NULL,
    inserted      INTEGER NOT NULL,
    duplicates    INTEGER NOT NULL,               
    dead_letters  INTEGER NOT NULL,                
    finished_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, batch_no),
    CHECK (inserted + duplicates + dead_letters = messages)           
);

CREATE TABLE dead_letter (                     
    kafka_topic     TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    error           TEXT NOT NULL,
    payload         TEXT,
    stored_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)          
);

CREATE INDEX ON change (changed_at);
CREATE INDEX ON change (wiki, changed_at);