CREATE TABLE blocks
(
    id                      SERIAL,
    block_hash              CHAR(64)        NOT NULL,
    accepting_block_id      BIGINT CHECK (accepting_block_id > 0) NULL,
    version                 INT             NOT NULL,
    hash_merkle_root        CHAR(64)        NOT NULL,
    accepted_id_merkle_root CHAR(64)        NOT NULL,
    utxo_commitment         CHAR(64)        NOT NULL,
    timestamp               TIMESTAMP(0)        NOT NULL,
    bits                    INT CHECK (bits >= 0)    NOT NULL,
    nonce                   BYTEA,
    blue_score              BIGINT CHECK (blue_score >= 0) NOT NULL,
    is_chain_block          BOOLEAN         NOT NULL,
    mass                    BIGINT          NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT idx_blocks_block_hash UNIQUE  (block_hash)
    ,
    CONSTRAINT fk_blocks_accepting_block_id
        FOREIGN KEY (accepting_block_id)
            REFERENCES blocks (id)
);

CREATE INDEX idx_blocks_timestamp ON blocks (timestamp);
CREATE INDEX idx_blocks_is_chain_block ON blocks (is_chain_block);