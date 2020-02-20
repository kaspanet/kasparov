CREATE SEQUENCE addresses_seq;

CREATE TABLE addresses
(
    id      BIGINT CHECK (id > 0) NOT NULL DEFAULT NEXTVAL ('addresses_seq'),
    address VARCHAR(64)        NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT idx_addresses_address UNIQUE  (address)
)
