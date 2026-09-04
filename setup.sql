CREATE SCHEMA IF NOT EXISTS cdc_lab;

CREATE TABLE IF NOT EXISTS cdc_lab.cdc_probe_small
(
    run_id       uuid        NOT NULL,
    worker_id    smallint    NOT NULL,
    seq           bigint      NOT NULL,
    source_ts     timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload       text        NOT NULL,
    PRIMARY KEY (run_id, worker_id, seq)
);

CREATE TABLE IF NOT EXISTS cdc_lab.cdc_probe_large
(
    run_id       uuid        NOT NULL,
    outcome      text        NOT NULL CHECK (outcome IN ('commit', 'rollback')),
    row_number   integer     NOT NULL,
    payload       text        NOT NULL,
    PRIMARY KEY (run_id, row_number)
);

COMMENT ON TABLE cdc_lab.cdc_probe_small IS
    'Small-transaction stream for the Managed Postgres CDC boundary lab';

COMMENT ON TABLE cdc_lab.cdc_probe_large IS
    'Single large transaction for the Managed Postgres CDC boundary lab';

