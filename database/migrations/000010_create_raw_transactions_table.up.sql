CREATE TABLE raw_transactions
(
    transaction_id   BIGINT NOT NULL,
    transaction_data BYTEA NOT NULL,
    PRIMARY KEY (transaction_id),
    CONSTRAINT fk_raw_transactions_transaction_id
        FOREIGN KEY (transaction_id)
            REFERENCES transactions (id)
);
