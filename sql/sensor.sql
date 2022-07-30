CREATE EXTENSION IF NOT EXISTS postgis;
DROP TABLE IF EXISTS node CASCADE;
-- 
CREATE TABLE IF NOT EXISTS node (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    , node_name TEXT UNIQUE -- appa1-gevo5
    , description text -- Appa 1 - S. Chiara
    , active boolean NOT NULL DEFAULT TRUE
    , lat numeric(6 , 4) NOT NULL CHECK (lat <= 90 AND lat >= - 90)
    , lon numeric(7 , 4) NOT NULL CHECK (lon <= 180 AND lon >= - 180)
    , geog Geography (point , 4326) GENERATED ALWAYS AS (ST_Makepoint (lon , lat)::geography) STORED
    , attrs jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX ON node USING gist (geog);


DROP TABLE IF EXISTS sensor CASCADE;
CREATE TABLE IF NOT EXISTS sensor (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    , node_id INT NOT NULL REFERENCES node(id) ON UPDATE CASCADE ON DELETE CASCADE
    , "name" text NOT NULL -- S1_ID
    , description text NOT NULL -- SnO2_c_MH20_L2
    , active boolean NOT NULL DEFAULT TRUE
    , attrs jsonb NOT NULL DEFAULT '{}'::jsonb
    , UNIQUE(node_id, "name")
);


DROP TABLE IF EXISTS packet CASCADE;
CREATE TABLE IF NOT EXISTS packet (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    , node_id INT NOT NULL REFERENCES node(id) ON UPDATE CASCADE ON DELETE CASCADE
    , insert_ts timestamp(0) with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
    , sensor_ts timestamp(0) with time zone NOT NULL
    , attrs jsonb NOT NULL DEFAULT '{}'::jsonb
);


DROP TABLE IF EXISTS packet_data CASCADE;
CREATE TABLE IF NOT EXISTS packet_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    , packet_id INT NOT NULL REFERENCES packet(id) ON UPDATE CASCADE ON DELETE CASCADE
    , sensor_id INT NOT NULL REFERENCES sensor(id) ON UPDATE CASCADE ON DELETE CASCADE
    , r1 DOUBLE PRECISION NOT NULL
    , r2 BIGINT NOT NULL
    , volt DOUBLE PRECISION NOT NULL
    , UNIQUE (packet_id, sensor_id)
);
