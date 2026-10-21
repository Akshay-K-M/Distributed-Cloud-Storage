-- 001_init.sql
CREATE TABLE IF NOT EXISTS objects (
  object_id        TEXT PRIMARY KEY,
  name             TEXT NOT NULL,
  size             INTEGER NOT NULL DEFAULT 0,
  content_type     TEXT NOT NULL DEFAULT 'application/octet-stream',
  checksum_sha256  TEXT NOT NULL DEFAULT '',
  chunk_count      INTEGER NOT NULL DEFAULT 0,
  state            TEXT NOT NULL CHECK (state IN ('UPLOADING','COMMITTED','DELETING')),
  created_at       INTEGER NOT NULL,
  modified_at      INTEGER NOT NULL,
  custom_metadata  TEXT NOT NULL DEFAULT '{}'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_name_committed ON objects(name) WHERE state = 'COMMITTED';
CREATE INDEX IF NOT EXISTS idx_objects_state ON objects(state);

CREATE TABLE IF NOT EXISTS chunks (
  chunk_id   TEXT PRIMARY KEY,
  object_id  TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  idx        INTEGER NOT NULL,
  size       INTEGER NOT NULL,
  sha256     TEXT NOT NULL,
  UNIQUE (object_id, idx)
);

CREATE TABLE IF NOT EXISTS chunk_replicas (
  chunk_id    TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
  worker_id   TEXT NOT NULL,
  state       TEXT NOT NULL CHECK (state IN ('OK','CORRUPT')),
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (chunk_id, worker_id)
);
CREATE INDEX IF NOT EXISTS idx_replicas_worker ON chunk_replicas(worker_id);

CREATE TABLE IF NOT EXISTS workers (
  worker_id           TEXT PRIMARY KEY,
  address             TEXT NOT NULL,
  status              TEXT NOT NULL CHECK (status IN ('ALIVE','SUSPECT','DEAD')),
  capacity_bytes      INTEGER NOT NULL DEFAULT 0,
  used_bytes          INTEGER NOT NULL DEFAULT 0,
  chunk_count         INTEGER NOT NULL DEFAULT 0,
  registered_at       INTEGER NOT NULL,
  last_status_change  INTEGER NOT NULL
);
