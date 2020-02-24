CREATE TABLE transactions
(
    id                 BIGSERIAL,
    accepting_block_id BIGINT NULL,
    transaction_hash   CHAR(64) NOT NULL,
    transaction_id     CHAR(64) NOT NULL,
    lock_time          BIGINT CHECK (lock_time >= 0) NOT NULL,
    subnetwork_id      BIGINT CHECK (subnetwork_id >= 0) NOT NULL,
    gas                BIGINT CHECK (gas >= 0) NOT NULL,
    payload_hash       CHAR(64) NOT NULL,
    payload            BYTEA NOT NULL,
    mass               BIGINT NOT NULL,
    version            INT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT idx_transactions_transaction_hash UNIQUE (transaction_hash),
    CONSTRAINT fk_transactions_accepting_block_id
        FOREIGN KEY (accepting_block_id)
            REFERENCES blocks (id)
);

CREATE INDEX idx_transactions_transaction_id ON transactions (transaction_id);
