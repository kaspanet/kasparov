CREATE TABLE `addresses`
(
    `id`      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `address` VARCHAR(64)        NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE INDEX `idx_addresses_address` (`address`)
)
