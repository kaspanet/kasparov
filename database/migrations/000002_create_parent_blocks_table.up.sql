CREATE TABLE parent_blocks
(
    block_id        BIGINT CHECK (block_id > 0) NOT NULL,
    parent_block_id BIGINT CHECK (parent_block_id > 0) NOT NULL,
    PRIMARY KEY (block_id, parent_block_id),
    CONSTRAINT fk_parent_blocks_block_id
        FOREIGN KEY (block_id)
            REFERENCES blocks (id),
    CONSTRAINT fk_parent_blocks_parent_block_id
        FOREIGN KEY (parent_block_id)
            REFERENCES blocks (id)
);
