/*
 * Teragrep Metadata Using HBase (hbs_03)
 * Copyright (C) 2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.hbs_03;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.JSON;
import org.jooq.Record18;
import org.jooq.SelectHavingStep;
import org.jooq.SelectSelectStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.MessageFormat;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.BUCKET;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.CATEGORY;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.HOST;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.LOGFILE;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.METADATA_VALUE;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.SOURCE_SYSTEM;
import static com.teragrep.hbs_03.jooq.generated.streamdb.Streamdb.STREAMDB;

public final class NestedTopNQueryByDate {

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedTopNQueryByDate.class);
    private final DSLContext ctx;
    private final String innerTableName;
    private final Date day;

    public NestedTopNQueryByDate(final DSLContext ctx, final Date day) {
        this(ctx, "inner_table", day);
    }

    public NestedTopNQueryByDate(final DSLContext ctx, final String innerTableName, final Date day) {
        this.ctx = ctx;
        this.innerTableName = innerTableName;
        this.day = day;
    }

    public Table<Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>> asTable() {
        final Table<?> innerTable = DSL.table(DSL.name(innerTableName));
        final Table<Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>> table;
        try (
                final SelectHavingStep<Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>> selectStep = selectStep()
        ) {
            table = selectStep.orderBy(LOGFILE.LOGDATE, LOGFILE.ID.asc()).asTable(innerTable);
        }
        LOGGER.info("Query as table <{}>", table);
        return table;
    }

    public SelectSelectStep<Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>> selectFields() {
        return ctx
                .select(LOGFILE.ID.as("id"), DSL.epoch(DSL.timestamp(LOGFILE.LOGDATE)).as("epoch"), LOGFILE.EXPIRATION.as("expiration"), LOGFILE.PATH.as("path"), LOGFILE.ORIGINAL_FILENAME.as("name"), DSL.timestamp(LOGFILE.ARCHIVED).as("archived"), LOGFILE.SHA256_CHECKSUM.as("checksum"), LOGFILE.ARCHIVE_ETAG.as("etag"), LOGFILE.LOGTAG.as("logtag"), LOGFILE.UNCOMPRESSED_FILE_SIZE.as("size"), DSL.jsonObjectAgg(METADATA_VALUE.VALUE_KEY, METADATA_VALUE.VALUE).as("meta"), SOURCE_SYSTEM.NAME.as("source"), CATEGORY.NAME.as("category"), BUCKET.NAME.as("bucket"), HOST.NAME.as("host"), STREAMDB.STREAM.TAG.as("stream_tag"), STREAMDB.LOG_GROUP.NAME.as("log_group"), STREAMDB.STREAM.DIRECTORY.as("directory"));
    }

    public SelectHavingStep<Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>> selectStep() {
        final Condition dateCondition = LOGFILE.LOGDATE.eq(DSL.date(day));
        return selectFields()
                .from(LOGFILE)
                .innerJoin(METADATA_VALUE)
                .on(LOGFILE.ID.eq(METADATA_VALUE.LOGFILE_ID))
                .innerJoin(SOURCE_SYSTEM)
                .on(LOGFILE.SOURCE_SYSTEM_ID.eq(SOURCE_SYSTEM.ID))
                .innerJoin(CATEGORY)
                .on(LOGFILE.CATEGORY_ID.eq(CATEGORY.ID))
                .innerJoin(BUCKET)
                .on(LOGFILE.BUCKET_ID.eq(BUCKET.ID))
                .innerJoin(HOST)
                .on(LOGFILE.HOST_ID.eq(HOST.ID))
                .innerJoin(STREAMDB.HOST)
                .on(HOST.NAME.eq(STREAMDB.HOST.NAME))
                .innerJoin(STREAMDB.LOG_GROUP)
                .on(STREAMDB.HOST.GID.eq(STREAMDB.LOG_GROUP.ID))
                .innerJoin(STREAMDB.STREAM)
                .on(STREAMDB.LOG_GROUP.ID.eq(STREAMDB.STREAM.GID))
                .where(dateCondition)
                .groupBy(LOGFILE.ID);
    }

    // legacy SQL for reference
    private String sql() {
        final String sql;
        final int limit = 10;
        boolean useLimit = true;
        if (useLimit) {
            sql = MessageFormat
                    .format(
                            "SELECT " + "`logfile`.`id` AS `id`, UNIX_TIMESTAMP(`logfile`.`logdate`) AS `epoch`,"
                                    + "    `logfile`.`expiration` AS `exp`," + "    `logfile`.`path` AS `pth`,"
                                    + "    `logfile`.`original_filename` AS `orig_nm`,"
                                    + "    UNIX_TIMESTAMP(`logfile`.`archived`) AS `archived`,"
                                    + "    `logfile`.`sha256_checksum` AS `checksum`,"
                                    + "    `logfile`.`archive_etag` AS `etag`," + "    `logfile`.`logtag` AS `logtag`,"
                                    + "    `logfile`.`uncompressed_file_size` AS `uncomp_sz`,"
                                    + "    JSON_OBJECTAGG(`metadata_value`.`value_key`, `metadata_value`.`value`) AS `meta`,"
                                    + "    `source_system`.`name` AS `source_nm`,"
                                    + "    `category`.`name` AS `ctg_nm`," + "    `bucket`.`name` AS `bckt_nm`,"
                                    + "    `host`.`name` AS `host_nm`,"
                                    + "    `{0}`.`stream`.`directory` AS `strm_dir`,"
                                    + "    `{0}`.`stream`.`tag` AS `strm_tag`,"
                                    + "    `{0}`.`log_group`.`name` AS `log_grp_nm`" + "FROM " + "    `logfile`"
                                    + "LEFT JOIN "
                                    + "    `metadata_value` ON `logfile`.`id` = `metadata_value`.`logfile_id`"
                                    + "LEFT JOIN "
                                    + "    `source_system` ON `logfile`.`source_system_id` = `source_system`.`id`"
                                    + "LEFT JOIN " + "    `category` ON `logfile`.`category_id` = `category`.`id`"
                                    + "LEFT JOIN " + "    `bucket` ON `logfile`.`bucket_id` = `bucket`.`id`"
                                    + "LEFT JOIN " + "    `host` ON `logfile`.`host_id` = `host`.`id`" + "LEFT JOIN "
                                    + "      `{0}`.`host` AS `stream_host` ON `host`.`name` = `stream_host`.`name`"
                                    + "LEFT JOIN "
                                    + "    `{0}`.`log_group` AS `log_group` ON `stream_host`.`gid` = `log_group`.`id`"
                                    + "LEFT JOIN "
                                    + "    `{0}`.`stream` AS `stream` ON `log_group`.`id` = `stream`.`gid`"
                                    + "GROUP BY " + "    `logfile`.`id`" + "LIMIT {1};",
                            STREAMDB.getName(), limit
                    );
        }
        else {
            sql = MessageFormat
                    .format(
                            "SELECT " + "`logfile`.`id` AS `id`, UNIX_TIMESTAMP(`logfile`.`logdate`) AS `epoch`,"
                                    + "    `logfile`.`expiration` AS `exp`," + "    `logfile`.`path` AS `pth`,"
                                    + "    `logfile`.`original_filename` AS `orig_nm`,"
                                    + "    UNIX_TIMESTAMP(`logfile`.`archived`) AS `archived`,"
                                    + "    `logfile`.`sha256_checksum` AS `checksum`,"
                                    + "    `logfile`.`archive_etag` AS `etag`," + "    `logfile`.`logtag` AS `logtag`,"
                                    + "    `logfile`.`uncompressed_file_size` AS `uncomp_sz`,"
                                    + "    JSON_OBJECTAGG(`metadata_value`.`value_key`, `metadata_value`.`value`) AS `meta`,"
                                    + "    `source_system`.`name` AS `source_nm`,"
                                    + "    `category`.`name` AS `ctg_nm`," + "    `bucket`.`name` AS `bckt_nm`,"
                                    + "    `host`.`name` AS `host_nm`,"
                                    + "    `{0}`.`stream`.`directory` AS `strm_dir`,"
                                    + "    `{0}`.`stream`.`tag` AS `strm_tag`,"
                                    + "    `{0}`.`log_group`.`name` AS `log_grp_nm`" + "FROM " + "    `logfile`"
                                    + "LEFT JOIN "
                                    + "    `metadata_value` ON `logfile`.`id` = `metadata_value`.`logfile_id`"
                                    + "LEFT JOIN "
                                    + "    `source_system` ON `logfile`.`source_system_id` = `source_system`.`id`"
                                    + "LEFT JOIN " + "    `category` ON `logfile`.`category_id` = `category`.`id`"
                                    + "LEFT JOIN" + "    `bucket` ON `logfile`.`bucket_id` = `bucket`.`id`"
                                    + "LEFT JOIN" + "    `host` ON `logfile`.`host_id` = `host`.`id`" + "LEFT JOIN"
                                    + "    `{0}`.`host` AS `stream_host` ON `host`.`name` = `stream_host`.`name`"
                                    + "LEFT JOIN"
                                    + "    `{0}`.`log_group` AS `log_group` ON `stream_host`.`gid` = `log_group`.`id`"
                                    + "LEFT JOIN"
                                    + "    `{0}`.`stream` AS `stream` ON `log_group`.`id` = `stream`.`gid`" + "GROUP BY"
                                    + "    `logfile`.`id`;",
                            STREAMDB.getName()
                    );
        }
        return sql;
    }
}
