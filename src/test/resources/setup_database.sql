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

DROP TABLE IF EXISTS `logtag`;
CREATE TABLE `logtag`
(
    `id`     bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID for logtag',
    `logtag` varchar(48)         NOT NULL COMMENT 'A link back to CFEngine',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uix_logtag` (`logtag`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 725
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci
  ROW_FORMAT = DYNAMIC COMMENT ='Contains logtag values that are identified using the ID';

DROP TABLE IF EXISTS `ci`;
CREATE TABLE `ci`
(
    `id`   bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID for ci',
    `name` varchar(255)        NOT NULL COMMENT 'Configuration item name of the logfile records',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uix_ci` (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci
  ROW_FORMAT = DYNAMIC COMMENT ='Contains ci values that are identified using the ID';

DROP TABLE IF EXISTS `logfile`;
CREATE TABLE `logfile`
(
    `id`                     bigint(20) unsigned  NOT NULL AUTO_INCREMENT,
    `logdate`                date                 NOT NULL COMMENT 'Log file''s date',
    `expiration`             date                 NOT NULL COMMENT 'Log file''s expiration date',
    `bucket_id`              smallint(5) unsigned NOT NULL COMMENT 'Reference to bucket table',
    `path`                   varchar(2048)        NOT NULL COMMENT 'Log file''s path in object storage',
    `object_key_hash`        char(64) GENERATED ALWAYS AS (sha2(concat(`path`, `bucket_id`), 256)) STORED COMMENT 'Hash of path and bucket_id for uniqueness checks. Known length: 64 characters (SHA-256)',
    `host_id`                smallint(5) unsigned NOT NULL COMMENT 'Reference to host table',
    `original_filename`      varchar(255)         NOT NULL COMMENT 'Log file''s original file name',
    `archived`               datetime             NOT NULL COMMENT 'Date and time when the log file was archived',
    `file_size`              bigint(20) unsigned  NOT NULL DEFAULT 0 COMMENT 'Log file''s size in bytes',
    `sha256_checksum`        char(44)             NOT NULL COMMENT 'An SHA256 hash of the log file (Note: known to be 44 characters long)',
    `archive_etag`           varchar(64)          NOT NULL COMMENT 'Object storage''s MD5 hash of the log file (Note: room left for possible implementation changes)',
    `logtag`                 varchar(48)          NOT NULL COMMENT 'A link back to CFEngine',
    `source_system_id`       smallint(5) unsigned NOT NULL COMMENT 'Log file''s source system (references source_system.id)',
    `category_id`            smallint(5) unsigned NOT NULL DEFAULT 0 COMMENT 'Log file''s category (references category.id)',
    `uncompressed_file_size` bigint(20) unsigned           DEFAULT NULL COMMENT 'Log file''s uncompressed file size',
    `epoch_hour`             bigint(20) unsigned           DEFAULT NULL COMMENT 'Log file''s epoch logdate',
    `epoch_expires`          bigint(20) unsigned           DEFAULT NULL COMMENT 'Log file''s epoch expiration',
    `epoch_archived`         bigint(20) unsigned           DEFAULT NULL COMMENT 'Log file''s epoch archived',
    `ci_id`                  bigint(20) unsigned           DEFAULT NULL COMMENT 'Log file''s foreign key to ci table',
    `logtag_id`              bigint(20) unsigned           DEFAULT NULL COMMENT 'Log file''s foreign key to logtag',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uix_logfile_object_hash` (`object_key_hash`),
    KEY `category_id` (`category_id`),
    KEY `ix_logfile_expiration` (`expiration`),
    KEY `ix_logfile__source_system_id` (`source_system_id`),
    KEY `cix_logfile_logdate_host_id_logtag` (`logdate`, `host_id`, `logtag`),
    KEY `host_id` (`host_id`),
    KEY `bucket_id` (`bucket_id`),
    KEY `cix_logfile_host_id_logtag_logdate` (`host_id`, `logtag`, `logdate`),
    KEY `cix_logfile_epoch_hour_host_id_logtag` (`epoch_hour`, `host_id`, `logtag`),
    KEY `ix_logfile_epoch_expires` (`epoch_expires`),
    KEY `fk_logfile__ci_id` (`ci_id`),
    KEY `fk_logfile__logtag_id` (`logtag_id`),
    CONSTRAINT `fk_logfile__ci_id` FOREIGN KEY (`ci_id`) REFERENCES `ci` (`id`),
    CONSTRAINT `fk_logfile__logtag_id` FOREIGN KEY (`logtag_id`) REFERENCES `logtag` (`id`),
    CONSTRAINT `fk_logfile__source_system_id` FOREIGN KEY (`source_system_id`) REFERENCES `source_system` (`id`),
    CONSTRAINT `logfile_ibfk_1` FOREIGN KEY (`bucket_id`) REFERENCES `bucket` (`id`),
    CONSTRAINT `logfile_ibfk_2` FOREIGN KEY (`host_id`) REFERENCES `journal_host` (`id`),
    CONSTRAINT `logfile_ibfk_4` FOREIGN KEY (`category_id`) REFERENCES `category` (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT ='Contains information for log files that have been run through Log Archiver';

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
start transaction;

-- 1. log_group
insert into log_group (name)
values ('group-10'),
       ('group-11'),
       ('group-12'),
       ('group-13'),
       ('group-14'),
       ('group-15'),
       ('group-16'),
       ('group-17'),
       ('group-18'),
       ('group-19')
on duplicate key update name=name;

-- 2. streamdb host
insert into host (name, gid)
values ('sc-99-99-10-10', (select id from log_group where name = 'group-10' limit 1)),
       ('sc-99-99-10-11', (select id from log_group where name = 'group-11' limit 1)),
       ('sc-99-99-10-12', (select id from log_group where name = 'group-12' limit 1)),
       ('sc-99-99-10-13', (select id from log_group where name = 'group-13' limit 1)),
       ('sc-99-99-10-14', (select id from log_group where name = 'group-14' limit 1)),
       ('sc-99-99-10-15', (select id from log_group where name = 'group-15' limit 1)),
       ('sc-99-99-10-16', (select id from log_group where name = 'group-16' limit 1)),
       ('sc-99-99-10-17', (select id from log_group where name = 'group-17' limit 1)),
       ('sc-99-99-10-18', (select id from log_group where name = 'group-18' limit 1)),
       ('sc-99-99-10-19', (select id from log_group where name = 'group-19' limit 1))
on duplicate key update gid = values(gid);

-- 3. journaldb host
insert into journal_host (name)
values ('sc-99-99-10-10'),
       ('sc-99-99-10-11'),
       ('sc-99-99-10-12'),
       ('sc-99-99-10-13'),
       ('sc-99-99-10-14'),
       ('sc-99-99-10-15'),
       ('sc-99-99-10-16'),
       ('sc-99-99-10-17'),
       ('sc-99-99-10-18'),
       ('sc-99-99-10-19')
on duplicate key update name=values(name);

-- 4. stream
insert into stream (gid, directory, stream, tag)
values ((select id from log_group where name = 'group-10' limit 1), 'cpu', 'log:cpu:0', '0ff11b44-cpu')
on duplicate key update stream=values(stream);

-- 5. bucket
insert into bucket (name)
values ('test-bucket')
on duplicate key update name=name;

-- 6. logtag
insert into logtag (logtag)
values ('0ff11b44-cpu')
on duplicate key update logtag=values(logtag);

-- 7. source_system
insert into source_system (name)
values ('log:cpu:0')
on duplicate key update name=values(name);

-- 8. category
insert into category (name)
values ('test-category')
on duplicate key update name=values(name);

-- 9. ci
insert into ci (name)
values ('ci-cpu')
on duplicate key update name=values(name);

commit;

create procedure insert_log(in i int)
begin
    declare v_logtag_value varchar(48) default '0ff11b44-cpu' collate utf8mb4_unicode_ci;
    declare v_logtag_id bigint unsigned;
    declare v_errmsg varchar(255) collate utf8mb4_unicode_ci;
    declare v_bucket_id smallint unsigned;
    declare v_host_id smallint unsigned;
    declare v_host_name varchar(255) default 'sc-99-99-10-10' collate utf8mb4_unicode_ci;
    declare v_source_system_id smallint unsigned;
    declare v_category_id smallint unsigned;
    declare v_ci_id bigint unsigned;

    select id
    into v_logtag_id
    from logtag
    where logtag = v_logtag_value collate utf8mb4_general_ci
    limit 1;

    if v_logtag_id is null then
        set v_errmsg = concat('logtag not found: ', v_logtag_value);
        signal sqlstate '45000' set message_text = v_errmsg;
    end if;

    select id
    into v_bucket_id
    from bucket
    where name = 'test-bucket' collate utf8mb4_general_ci
    limit 1;

    select id
    into v_host_id
    from journal_host
    where name = v_host_name collate utf8mb4_general_ci
    limit 1;

    select id
    into v_source_system_id
    from source_system
    where name = 'log:cpu:0' collate utf8mb4_general_ci
    limit 1;

    select id
    into v_category_id
    from category
    where name = 'test-category' collate utf8mb4_general_ci
    limit 1;

    select id
    into v_ci_id
    from ci
    where name = 'ci-cpu' collate utf8mb4_general_ci
    limit 1;

    insert into logfile (logdate, expiration, bucket_id, path, host_id, original_filename, archived,
                         file_size, sha256_checksum, archive_etag, logtag, source_system_id, category_id,
                         uncompressed_file_size, epoch_hour, epoch_expires, epoch_archived, ci_id, logtag_id)
    select curdate()                                                     as logdate,
           date_add(curdate(), interval 1 year)                          as expiration,
           v_bucket_id                                                   as bucket_id,
           concat(
                   date_format(curdate(), '%Y/%m-%d'), '/',
                   (select name from journal_host where id = v_host_id), '/',
                   v_logtag_value, '/',
                   'cpu-', date_format(curdate(), '%Y%m%d%H'), '_', i, '.log.gz'
           )                                                             as path,
           v_host_id                                                     as host_id,
           concat('test-', i, '.log')                                    as original_filename,
           now()                                                         as archived,
           floor(rand() * 1000000)                                       as file_size,
           lpad(conv(floor(rand() * pow(36, 10)), 10, 36), 44, '0')      as sha256_checksum,
           lpad(conv(floor(rand() * pow(36, 10)), 10, 36), 64, '0')      as archive_etag,
           v_logtag_value                                                as logtag,
           v_source_system_id                                            as source_system_id,
           v_category_id                                                 as category_id,
           floor(rand() * 2000000)                                       as uncompressed_file_size,
           unix_timestamp(curdate()) div 3600                            as epoch_hour,
           unix_timestamp(date_add(curdate(), interval 1 year)) div 3600 as epoch_expires,
           unix_timestamp(now()) div 3600                                as epoch_archived,
           v_ci_id                                                       as ci_id,
           v_logtag_id                                                   as logtag_id;
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
