CREATE TABLE accepted_blocks
(
    block_id        BIGINT NOT NULL,
    accepted_block_id BIGINT NOT NULL,
    PRIMARY KEY (block_id, accepted_block_id),
    CONSTRAINT fk_accepted_blocks_block_id
        FOREIGN KEY (block_id)
            REFERENCES blocks (id),
    CONSTRAINT fk_accepted_blocks_accepted_block_id
        FOREIGN KEY (accepted_block_id)
            REFERENCES blocks (id)
);
