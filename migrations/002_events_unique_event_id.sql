-- event_id is stable across provider redeliveries, so it must be unique.
-- Clear any duplicates left by the pre-fix check-then-insert path first,
-- keeping the earliest delivery of each event.
DELETE FROM events a USING events b
WHERE a.id > b.id AND a.event_id = b.event_id;

CREATE UNIQUE INDEX IF NOT EXISTS events_event_id_key ON events (event_id);