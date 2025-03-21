DROP TABLE IF EXISTS `bucket`;
CREATE TABLE `bucket`
(
    `id`   smallint(5) unsigned                   NOT NULL AUTO_INCREMENT,
    `name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Name of the bucket',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uix_bucket_name` (`name`)
);

DROP TABLE IF EXISTS `journal_host`;
CREATE TABLE `journal_host`
(
    `id`   smallint(5) unsigned                    NOT NULL AUTO_INCREMENT,
    `name` varchar(175) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Name of the host',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uix_journal_host_name` (`name`)
);

DROP TABLE IF EXISTS `category`;
CREATE TABLE `category`
(
    `id`   smallint(5) unsigned NOT NULL AUTO_INCREMENT,
    `name` varchar(175) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Category''s name',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uix_category_name` (`name`)
);

DROP TABLE IF EXISTS `source_system`;
CREATE TABLE `source_system`
(
    `id`   smallint(5) unsigned                    NOT NULL AUTO_INCREMENT,
    `name` varchar(175) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Source system''s name',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uix_source_system_name` (`name`)
);

DROP TABLE IF EXISTS `logfile`;
CREATE TABLE `logfile`
(
    `id`                     bigint(20) unsigned                      NOT NULL AUTO_INCREMENT,
    `logdate`                date                                     NOT NULL COMMENT 'Log file''s date',
    `expiration`             date                                     NOT NULL COMMENT 'Log file''s expiration date',
    `bucket_id`              smallint(5) unsigned                     NOT NULL COMMENT 'Reference to bucket table',
    `path`                   varchar(2048) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Log file''s path in object storage',
    `object_key_hash`        char(64) GENERATED ALWAYS AS (sha2(concat(`path`, `bucket_id`), 256)) STORED COMMENT 'Hash of path and bucket_id for uniqueness checks. Known length: 64 characters (SHA-256)',
    `host_id`                smallint(5) unsigned                     NOT NULL COMMENT 'Reference to host table',
    `original_filename`      varchar(255) COLLATE utf8mb4_unicode_ci  NOT NULL COMMENT 'Log file''s original file name',
    `archived`               datetime                                 NOT NULL COMMENT 'Date and time when the log file was archived',
    `file_size`              bigint(20) unsigned                      NOT NULL DEFAULT 0 COMMENT 'Log file''s size in bytes',
    `sha256_checksum`        char(44) COLLATE utf8mb4_unicode_ci      NOT NULL COMMENT 'An SHA256 hash of the log file (Note: known to be 44 characters long)',
    `archive_etag`           varchar(64) COLLATE utf8mb4_unicode_ci   NOT NULL COMMENT 'Object storage''s MD5 hash of the log file (Note: room left for possible implementation changes)',
    `logtag`                 varchar(48) COLLATE utf8mb4_unicode_ci   NOT NULL COMMENT 'A link back to CFEngine',
    `source_system_id`       smallint(5) unsigned                     NOT NULL COMMENT 'Log file''s source system (references source_system.id)',
    `category_id`            smallint(5) unsigned                     NOT NULL DEFAULT 0 COMMENT 'Log file''s category (references category.id)',
    `uncompressed_file_size` bigint(20) unsigned                               DEFAULT NULL COMMENT 'Log file''s  uncompressed file size',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uix_logfile_object_hash` (`object_key_hash`),
    KEY `bucket_id` (`bucket_id`),
    KEY `journal_host_id` (`host_id`),
    KEY `category_id` (`category_id`),
    KEY `ix_logfile_expiration` (`expiration`),
    KEY `ix_logfile__source_system_id` (`source_system_id`),
    KEY `cix_logfile_logdate_host_id_logtag` (`logdate`, `host_id`, `logtag`),
    CONSTRAINT `fk_logfile__source_system_id` FOREIGN KEY (`source_system_id`) REFERENCES `source_system` (`id`),
    CONSTRAINT `logfile_ibfk_1` FOREIGN KEY (`bucket_id`) REFERENCES `bucket` (`id`),
    CONSTRAINT `logfile_ibfk_2` FOREIGN KEY (`host_id`) REFERENCES `journal_host` (`id`),
    CONSTRAINT `logfile_ibfk_4` FOREIGN KEY (`category_id`) REFERENCES `category` (`id`)
);


CREATE TABLE `corrupted_archive`
(
    `logfile_id` bigint(20) unsigned NOT NULL COMMENT 'The logfile that is the corrupted archive (references logfile.id).',
    PRIMARY KEY (`logfile_id`),
    CONSTRAINT `corrupted_archive_ibfk_1` FOREIGN KEY (`logfile_id`) REFERENCES `logfile` (`id`)
);

DROP TABLE IF EXISTS `flyway_schema_history`;
CREATE TABLE `flyway_schema_history`
(
    `installed_rank` int(11)                                  NOT NULL,
    `version`        varchar(50) COLLATE utf8mb4_unicode_ci            DEFAULT NULL,
    `description`    varchar(200) COLLATE utf8mb4_unicode_ci  NOT NULL,
    `type`           varchar(20) COLLATE utf8mb4_unicode_ci   NOT NULL,
    `script`         varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL,
    `checksum`       int(11)                                           DEFAULT NULL,
    `installed_by`   varchar(100) COLLATE utf8mb4_unicode_ci  NOT NULL,
    `installed_on`   timestamp                                NOT NULL DEFAULT current_timestamp(),
    `execution_time` int(11)                                  NOT NULL,
    `success`        tinyint(1)                               NOT NULL,
    PRIMARY KEY (`installed_rank`),
    KEY `flyway_schema_history_s_idx` (`success`)
);

DROP TABLE IF EXISTS `metadata_value`;
CREATE TABLE `metadata_value`
(
    `id`         bigint(20) unsigned                     NOT NULL AUTO_INCREMENT,
    `logfile_id` bigint(20) unsigned                     NOT NULL COMMENT 'Foreign key referencing Logfile.id',
    `value_key`  varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Identifier key for the attribute',
    `value`      varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Value of the attribute',
    PRIMARY KEY (`id`),
    KEY `logfile_id` (`logfile_id`),
    CONSTRAINT `metadata_value_ibfk_1` FOREIGN KEY (`logfile_id`) REFERENCES `logfile` (`id`)
);


DROP TABLE IF EXISTS `restore_job`;
CREATE TABLE `restore_job`
(
    `job_id`     varchar(768) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Job id from aws glacier',
    `logfile_id` bigint(20) unsigned                     NOT NULL COMMENT 'Reference to logfile which is going to be restored',
    `created`    datetime                                NOT NULL COMMENT 'Job creation time',
    `task_id`    varchar(5) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Task id this job belongs to',
    PRIMARY KEY (`job_id`),
    KEY `logfile_id` (`logfile_id`),
    CONSTRAINT `restore_job_ibfk_1` FOREIGN KEY (`logfile_id`) REFERENCES `logfile` (`id`)
);

DROP TABLE IF EXISTS `log_group`;
CREATE TABLE `log_group`
(
    `id`   int(10) unsigned                        NOT NULL AUTO_INCREMENT,
    `name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`id`)
);

DROP TABLE IF EXISTS `host`;
CREATE TABLE `host`
(
    `id`   int(10) unsigned                        NOT NULL AUTO_INCREMENT,
    `name` varchar(175) COLLATE utf8mb4_unicode_ci NOT NULL,
    `gid`  int(10) unsigned                        NOT NULL,
    PRIMARY KEY (`id`),
    KEY `gid` (`gid`),
    KEY `idx_name_id` (`name`, `id`),
    CONSTRAINT `host_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE
);


DROP TABLE IF EXISTS `stream`;
CREATE TABLE `stream`
(
    `id`        int(10) unsigned                        NOT NULL AUTO_INCREMENT,
    `gid`       int(10) unsigned                        NOT NULL,
    `directory` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    `stream`    varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
    `tag`       varchar(48) COLLATE utf8mb4_unicode_ci  NOT NULL,
    PRIMARY KEY (`id`),
    KEY `gid` (`gid`),
    CONSTRAINT `stream_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE
);

## INSERTS

START TRANSACTION;
DELETE
FROM log_group;
DELETE
FROM host;
DELETE
FROM stream;
insert into log_group (name)
values ('group-10');
insert into host (name, gid)
values ('sc-99-99-10-10', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-11', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-12', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-13', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-14', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-15', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-16', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-17', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-18', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-19', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-20', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-21', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-22', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-23', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-24', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-25', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-26', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-27', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-28', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-29', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-30', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-31', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-32', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-33', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-34', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-35', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-36', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-37', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-38', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-39', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-40', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-41', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-42', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-43', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-44', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-45', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-46', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-47', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-48', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-49', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-50', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-51', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-52', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-53', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-54', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-55', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-56', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-57', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-58', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-59', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-60', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-61', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-62', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-63', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-64', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-65', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-66', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-67', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-68', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-69', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-70', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-71', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-72', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-73', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-74', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-75', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-76', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-77', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-78', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-79', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-80', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-81', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-82', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-83', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-84', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-85', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-86', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-87', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-88', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-89', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-90', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-91', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-92', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-93', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-94', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-95', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-96', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-97', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-98', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-99', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-100', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-101', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-102', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-103', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-104', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-105', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-106', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-107', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-108', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-109', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-110', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-111', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-112', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-113', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-114', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-115', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-116', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-117', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-118', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-119', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-120', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-121', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-122', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-123', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-124', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-125', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-126', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-127', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-128', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-129', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-130', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-131', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-132', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-133', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-134', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-135', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-136', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-137', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-138', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-139', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-140', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-141', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-142', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-143', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-144', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-145', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-146', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-147', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-148', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-149', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-150', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-151', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-152', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-153', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-154', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-155', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-156', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-157', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-158', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-159', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-160', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-161', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-162', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-163', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-164', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-165', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-166', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-167', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-168', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-169', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-170', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-171', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-172', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-173', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-174', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-175', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-176', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-177', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-178', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-179', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-180', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-181', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-182', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-183', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-184', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-185', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-186', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-187', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-188', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-189', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-190', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-191', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-192', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-193', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-194', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-195', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-196', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-197', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-198', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-199', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-200', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-201', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-202', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-203', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-204', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-205', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-206', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-207', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-208', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-209', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-210', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-211', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-212', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-213', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-214', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-215', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-216', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-217', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-218', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-219', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-220', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-221', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-222', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-223', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-224', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-225', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-226', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-227', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-228', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-229', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-230', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-231', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-232', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-233', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-234', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-235', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-236', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-237', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-238', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-239', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-240', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-241', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-242', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-243', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-244', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-245', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-246', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-247', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-248', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-249', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-250', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-251', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-252', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-253', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-254', (select id from log_group where name = 'group-10'));
insert into host (name, gid)
values ('sc-99-99-10-255', (select id from log_group where name = 'group-10'));
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'cpu', 'log:cpu:0', '0ff11b44-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'storage', 'log:fs-block:0', '8411b757-fs-block');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'storage', 'log:fs-inode:0', 'dd731d68-fs-inode');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'storage', 'log:io:0', 'afe23b85-io');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'network', 'log:net:0', '124f76f0-net');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'cpu', 'log:pid-cpu:0', '75c32eb2-pid-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'storage', 'log:pid-disk:0', '62a28466-pid-disk');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'memory', 'log:pid-page-memory:0',
        '5a67ffae-pid-page-memory');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'memory', 'log:vmstat:0', '7ee85a32-vmstat');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10'), 'generated-data', 'log:generated-data:0', 'generated-data');
insert into log_group (name)
values ('group-11');
insert into host (name, gid)
values ('sc-99-99-11-10', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-11', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-12', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-13', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-14', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-15', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-16', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-17', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-18', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-19', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-20', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-21', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-22', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-23', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-24', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-25', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-26', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-27', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-28', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-29', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-30', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-31', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-32', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-33', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-34', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-35', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-36', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-37', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-38', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-39', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-40', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-41', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-42', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-43', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-44', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-45', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-46', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-47', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-48', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-49', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-50', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-51', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-52', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-53', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-54', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-55', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-56', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-57', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-58', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-59', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-60', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-61', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-62', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-63', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-64', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-65', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-66', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-67', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-68', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-69', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-70', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-71', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-72', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-73', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-74', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-75', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-76', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-77', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-78', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-79', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-80', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-81', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-82', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-83', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-84', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-85', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-86', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-87', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-88', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-89', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-90', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-91', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-92', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-93', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-94', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-95', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-96', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-97', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-98', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-99', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-100', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-101', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-102', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-103', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-104', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-105', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-106', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-107', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-108', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-109', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-110', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-111', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-112', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-113', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-114', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-115', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-116', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-117', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-118', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-119', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-120', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-121', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-122', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-123', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-124', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-125', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-126', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-127', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-128', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-129', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-130', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-131', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-132', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-133', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-134', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-135', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-136', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-137', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-138', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-139', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-140', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-141', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-142', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-143', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-144', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-145', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-146', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-147', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-148', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-149', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-150', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-151', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-152', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-153', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-154', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-155', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-156', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-157', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-158', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-159', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-160', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-161', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-162', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-163', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-164', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-165', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-166', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-167', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-168', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-169', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-170', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-171', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-172', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-173', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-174', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-175', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-176', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-177', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-178', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-179', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-180', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-181', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-182', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-183', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-184', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-185', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-186', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-187', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-188', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-189', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-190', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-191', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-192', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-193', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-194', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-195', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-196', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-197', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-198', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-199', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-200', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-201', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-202', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-203', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-204', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-205', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-206', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-207', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-208', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-209', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-210', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-211', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-212', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-213', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-214', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-215', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-216', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-217', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-218', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-219', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-220', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-221', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-222', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-223', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-224', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-225', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-226', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-227', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-228', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-229', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-230', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-231', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-232', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-233', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-234', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-235', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-236', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-237', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-238', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-239', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-240', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-241', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-242', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-243', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-244', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-245', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-246', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-247', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-248', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-249', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-250', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-251', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-252', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-253', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-254', (select id from log_group where name = 'group-11'));
insert into host (name, gid)
values ('sc-99-99-11-255', (select id from log_group where name = 'group-11'));
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'cpu', 'log:cpu:0', '0ff11b44-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'storage', 'log:fs-block:0', '8411b757-fs-block');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'storage', 'log:fs-inode:0', 'dd731d68-fs-inode');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'storage', 'log:io:0', 'afe23b85-io');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'network', 'log:net:0', '124f76f0-net');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'cpu', 'log:pid-cpu:0', '75c32eb2-pid-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'storage', 'log:pid-disk:0', '62a28466-pid-disk');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'memory', 'log:pid-page-memory:0',
        '5a67ffae-pid-page-memory');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'memory', 'log:vmstat:0', '7ee85a32-vmstat');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-11'), 'generated-data', 'log:generated-data:0', 'generated-data');
insert into log_group (name)
values ('group-12');
insert into host (name, gid)
values ('sc-99-99-12-10', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-11', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-12', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-13', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-14', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-15', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-16', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-17', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-18', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-19', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-20', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-21', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-22', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-23', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-24', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-25', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-26', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-27', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-28', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-29', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-30', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-31', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-32', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-33', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-34', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-35', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-36', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-37', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-38', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-39', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-40', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-41', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-42', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-43', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-44', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-45', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-46', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-47', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-48', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-49', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-50', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-51', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-52', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-53', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-54', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-55', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-56', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-57', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-58', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-59', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-60', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-61', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-62', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-63', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-64', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-65', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-66', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-67', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-68', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-69', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-70', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-71', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-72', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-73', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-74', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-75', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-76', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-77', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-78', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-79', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-80', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-81', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-82', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-83', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-84', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-85', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-86', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-87', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-88', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-89', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-90', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-91', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-92', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-93', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-94', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-95', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-96', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-97', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-98', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-99', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-100', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-101', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-102', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-103', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-104', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-105', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-106', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-107', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-108', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-109', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-110', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-111', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-112', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-113', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-114', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-115', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-116', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-117', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-118', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-119', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-120', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-121', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-122', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-123', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-124', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-125', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-126', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-127', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-128', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-129', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-130', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-131', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-132', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-133', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-134', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-135', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-136', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-137', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-138', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-139', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-140', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-141', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-142', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-143', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-144', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-145', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-146', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-147', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-148', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-149', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-150', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-151', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-152', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-153', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-154', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-155', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-156', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-157', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-158', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-159', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-160', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-161', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-162', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-163', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-164', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-165', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-166', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-167', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-168', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-169', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-170', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-171', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-172', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-173', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-174', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-175', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-176', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-177', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-178', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-179', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-180', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-181', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-182', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-183', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-184', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-185', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-186', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-187', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-188', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-189', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-190', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-191', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-192', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-193', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-194', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-195', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-196', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-197', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-198', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-199', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-200', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-201', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-202', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-203', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-204', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-205', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-206', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-207', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-208', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-209', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-210', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-211', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-212', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-213', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-214', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-215', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-216', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-217', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-218', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-219', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-220', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-221', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-222', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-223', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-224', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-225', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-226', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-227', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-228', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-229', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-230', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-231', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-232', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-233', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-234', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-235', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-236', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-237', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-238', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-239', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-240', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-241', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-242', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-243', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-244', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-245', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-246', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-247', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-248', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-249', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-250', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-251', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-252', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-253', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-254', (select id from log_group where name = 'group-12'));
insert into host (name, gid)
values ('sc-99-99-12-255', (select id from log_group where name = 'group-12'));
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'cpu', 'log:cpu:0', '0ff11b44-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'storage', 'log:fs-block:0', '8411b757-fs-block');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'storage', 'log:fs-inode:0', 'dd731d68-fs-inode');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'storage', 'log:io:0', 'afe23b85-io');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'network', 'log:net:0', '124f76f0-net');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'cpu', 'log:pid-cpu:0', '75c32eb2-pid-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'storage', 'log:pid-disk:0', '62a28466-pid-disk');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'memory', 'log:pid-page-memory:0',
        '5a67ffae-pid-page-memory');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'memory', 'log:vmstat:0', '7ee85a32-vmstat');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-12'), 'generated-data', 'log:generated-data:0', 'generated-data');
insert into log_group (name)
values ('group-13');
insert into host (name, gid)
values ('sc-99-99-13-10', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-11', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-12', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-13', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-14', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-15', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-16', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-17', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-18', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-19', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-20', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-21', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-22', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-23', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-24', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-25', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-26', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-27', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-28', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-29', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-30', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-31', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-32', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-33', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-34', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-35', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-36', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-37', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-38', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-39', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-40', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-41', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-42', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-43', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-44', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-45', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-46', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-47', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-48', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-49', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-50', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-51', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-52', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-53', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-54', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-55', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-56', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-57', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-58', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-59', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-60', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-61', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-62', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-63', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-64', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-65', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-66', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-67', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-68', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-69', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-70', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-71', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-72', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-73', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-74', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-75', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-76', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-77', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-78', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-79', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-80', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-81', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-82', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-83', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-84', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-85', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-86', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-87', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-88', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-89', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-90', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-91', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-92', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-93', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-94', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-95', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-96', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-97', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-98', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-99', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-100', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-101', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-102', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-103', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-104', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-105', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-106', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-107', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-108', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-109', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-110', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-111', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-112', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-113', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-114', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-115', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-116', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-117', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-118', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-119', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-120', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-121', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-122', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-123', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-124', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-125', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-126', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-127', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-128', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-129', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-130', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-131', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-132', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-133', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-134', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-135', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-136', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-137', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-138', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-139', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-140', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-141', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-142', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-143', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-144', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-145', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-146', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-147', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-148', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-149', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-150', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-151', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-152', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-153', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-154', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-155', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-156', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-157', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-158', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-159', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-160', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-161', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-162', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-163', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-164', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-165', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-166', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-167', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-168', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-169', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-170', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-171', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-172', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-173', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-174', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-175', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-176', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-177', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-178', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-179', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-180', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-181', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-182', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-183', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-184', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-185', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-186', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-187', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-188', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-189', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-190', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-191', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-192', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-193', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-194', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-195', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-196', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-197', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-198', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-199', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-200', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-201', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-202', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-203', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-204', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-205', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-206', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-207', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-208', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-209', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-210', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-211', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-212', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-213', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-214', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-215', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-216', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-217', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-218', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-219', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-220', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-221', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-222', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-223', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-224', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-225', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-226', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-227', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-228', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-229', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-230', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-231', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-232', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-233', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-234', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-235', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-236', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-237', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-238', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-239', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-240', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-241', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-242', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-243', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-244', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-245', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-246', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-247', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-248', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-249', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-250', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-251', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-252', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-253', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-254', (select id from log_group where name = 'group-13'));
insert into host (name, gid)
values ('sc-99-99-13-255', (select id from log_group where name = 'group-13'));
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'cpu', 'log:cpu:0', '0ff11b44-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'storage', 'log:fs-block:0', '8411b757-fs-block');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'storage', 'log:fs-inode:0', 'dd731d68-fs-inode');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'storage', 'log:io:0', 'afe23b85-io');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'network', 'log:net:0', '124f76f0-net');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'cpu', 'log:pid-cpu:0', '75c32eb2-pid-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'storage', 'log:pid-disk:0', '62a28466-pid-disk');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'memory', 'log:pid-page-memory:0',
        '5a67ffae-pid-page-memory');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'memory', 'log:vmstat:0', '7ee85a32-vmstat');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-13'), 'generated-data', 'log:generated-data:0', 'generated-data');
insert into log_group (name)
values ('group-14');
insert into host (name, gid)
values ('sc-99-99-14-10', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-11', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-12', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-13', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-14', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-15', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-16', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-17', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-18', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-19', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-20', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-21', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-22', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-23', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-24', (select id from log_group where name = 'group-14'));
insert into host (name, gid)
values ('sc-99-99-14-25', (select id from log_group where name = 'group-14'));
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'cpu', 'log:cpu:0', '0ff11b44-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'storage', 'log:fs-block:0', '8411b757-fs-block');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'storage', 'log:fs-inode:0', 'dd731d68-fs-inode');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'storage', 'log:io:0', 'afe23b85-io');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'network', 'log:net:0', '124f76f0-net');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'cpu', 'log:pid-cpu:0', '75c32eb2-pid-cpu');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'storage', 'log:pid-disk:0', '62a28466-pid-disk');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'memory', 'log:pid-page-memory:0',
        '5a67ffae-pid-page-memory');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'memory', 'log:vmstat:0', '7ee85a32-vmstat');
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-14'), 'generated-data', 'log:generated-data:0', 'generated-data');

insert into bucket (name)
values ('test-bucket')
on duplicate key update id = id;
insert into category (name)
values ('test-category')
on duplicate key update id = id;

insert into journal_host (name)
select host.name from host
on duplicate key update name = journal_host.name;

insert into source_system (name)
values ('log:cpu:0')
on duplicate key update id = id;

COMMIT;

create procedure insert_log(in i int)
begin
    insert into logfile (logdate, expiration, bucket_id, path, host_id, original_filename, archived,
                         file_size, sha256_checksum, archive_etag, logtag, source_system_id, category_id,
                         uncompressed_file_size)
    values (curdate(), date_add(curdate(), interval 1 year),
            (select id from bucket where name = 'test-bucket' limit 1),
            concat('/logs/generated-', i, '.log'),
            (select id from journal_host order by rand() limit 1),
            concat('test-', i, '.log'),
            now(),
            floor(rand() * 1000000),
            lpad(conv(floor(rand() * pow(36, 10)), 10, 36), 44, '0'),
            lpad(conv(floor(rand() * pow(36, 10)), 10, 36), 64, '0'),
            '0ff11b44-cpu',
            (select id from source_system where name = 'log:cpu:0' limit 1),
            (select id from category where name = 'test-category' limit 1),
            floor(rand() * 2000000));
end;

create procedure insert_logs(in start_i int, in end_i int)
begin
    declare i int default start_i;

    start transaction;

    while i <= end_i
        do
            call insert_log(i);
            set i = i + 1;
        end while;
    commit;
end;

call insert_logs(1, 10000);
