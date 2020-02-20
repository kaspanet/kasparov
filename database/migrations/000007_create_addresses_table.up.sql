CREATE TABLE addresses
(
    id      SERIAL,
    address VARCHAR(64)        NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT idx_addresses_address UNIQUE  (address)
)
