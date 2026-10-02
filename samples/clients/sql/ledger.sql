-- Receipt ledger for the client samples.
-- SQL does not open the NuvexaMQ data port. Each language sample prints
-- "published <stream> partition <n> offset <n>"; store that receipt here.

CREATE TABLE nuvexa_receipt (
    language text PRIMARY KEY,
    stream text NOT NULL,
    partition_id integer NOT NULL,
    offset_value bigint NOT NULL,
    payload text NOT NULL,
    recorded_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Example after `python3 samples/clients/python/demo.py` prints offset 0:
-- INSERT INTO nuvexa_receipt (language, stream, partition_id, offset_value, payload)
-- VALUES ('python', 'clients', 0, 0, 'hello from python');
