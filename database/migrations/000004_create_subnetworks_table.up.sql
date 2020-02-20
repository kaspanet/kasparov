CREATE SEQUENCE subnetworks_seq;

CREATE TABLE subnetworks
(
    id            BIGINT CHECK (id > 0) NOT NULL DEFAULT NEXTVAL ('subnetworks_seq'),
    subnetwork_id CHAR(40)        NOT NULL,
    gas_limit     BIGINT CHECK (gas_limit >= 0) NULL,
    PRIMARY KEY (id),
    CONSTRAINT idx_subnetworks_subnetwork_id UNIQUE  (subnetwork_id)
);
