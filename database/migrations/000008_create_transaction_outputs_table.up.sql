CREATE TABLE transaction_outputs
(
    id             SERIAL,
    transaction_id BIGINT CHECK (transaction_id > 0) NOT NULL,
    index          BIGINT CHECK (index >= 0)    NOT NULL,
    value          BIGINT CHECK (value >= 0) NOT NULL,
    script_pub_key BYTEA            NOT NULL,
    is_spent       BOOLEAN         NOT NULL,
    address_id     BIGINT CHECK (address_id > 0) NULL,
    PRIMARY KEY (id)
    ,
    CONSTRAINT fk_transaction_outputs_transaction_id
        FOREIGN KEY (transaction_id)
            REFERENCES transactions (id),
    CONSTRAINT fk_transaction_outputs_address_id
        FOREIGN KEY (address_id)
            REFERENCES addresses (id)
);

CREATE INDEX idx_transaction_outputs_transaction_id ON transaction_outputs (transaction_id);
