CREATE TABLE transactions_to_blocks
(
    transaction_id    BIGINT CHECK (transaction_id > 0) NOT NULL,
    block_id          BIGINT CHECK (block_id > 0) NOT NULL,
    index INT CHECK (index >= 0)    NOT NULL,
    PRIMARY KEY (transaction_id, block_id),
    CONSTRAINT fk_transactions_to_blocks_block_id
        FOREIGN KEY (block_id)
            REFERENCES blocks (id),
    CONSTRAINT fk_transactions_to_blocks_transaction_id
        FOREIGN KEY (transaction_id)
            REFERENCES transactions (id)
);

CREATE INDEX idx_transactions_to_blocks_index ON transactions_to_blocks (index);
