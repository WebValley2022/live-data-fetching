DROP TABLE IF EXISTS appa_data CASCADE;
CREATE TABLE IF NOT EXISTS appa_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    , stazione TEXT NOT NULL
    , inquinante TEXT NOT NULL
    , ts timestamp(0) with time zone NOT NULL
    , valore BIGINT NOT NULL
    , UNIQUE (stazione, inquinante, ts)
);
