CREATE TABLE transaction_inputs
(
    id                             BIGSERIAL,
    transaction_id                 BIGINT NULL,
    previous_transaction_output_id BIGINT NOT NULL,
    index                          INT CHECK (index >= 0) NOT NULL,
    signature_script               BYTEA NOT NULL,
    sequence                       BIGINT CHECK (sequence >= 0) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_transaction_inputs_transaction_id
        FOREIGN KEY (transaction_id)
            REFERENCES transactions (id),
    CONSTRAINT fk_transaction_inputs_previous_transaction_output_id
        FOREIGN KEY (previous_transaction_output_id)
            REFERENCES transaction_outputs (id)
);

CREATE INDEX idx_transaction_inputs_transaction_id ON transaction_inputs (transaction_id);
CREATE INDEX idx_transaction_inputs_previous_transaction_output_id ON transaction_inputs (previous_transaction_output_id);
