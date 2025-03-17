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
package com.teragrep.hbs_03.sql;

import com.teragrep.hbs_03.hbase.MetaRow;
import com.teragrep.hbs_03.hbase.Row;
import org.jooq.DSLContext;
import org.jooq.Explain;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record21;
import org.jooq.Result;
import org.jooq.SelectOnConditionStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;
import static com.teragrep.hbs_03.jooq.generated.streamdb.Streamdb.STREAMDB;

/**
 * Flat query with all the fields selected for migration
 */
public final class LogfileTableFlatQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogfileTableFlatQuery.class);
    private final DSLContext ctx;
    private final Table<Record1<ULong>> rangeIdTable;
    private final HostMappingTable hostMappingTable;

    public LogfileTableFlatQuery(final DSLContext ctx, final long startId, final long endId) {
        this(ctx, new LogfileTableIdBatchQuery(ctx, startId, endId).asTable(), new HostMappingTable(ctx));
    }

    public LogfileTableFlatQuery(
            final DSLContext ctx,
            final Table<Record1<ULong>> rangeIdTable,
            final HostMappingTable hostMappingTable
    ) {
        this.ctx = ctx;
        this.rangeIdTable = rangeIdTable;
        this.hostMappingTable = hostMappingTable;
    }

    private Field<Long> logTimeFunctionField() {
        final String dateFromPathRegex = "UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR({0},'^\\\\d{4}\\\\/\\\\d{2}-\\\\d{2}\\\\/[\\\\w\\\\.-]+\\\\/([^\\\\p{Z}\\\\p{C}]+?)\\\\/([^\\\\p{Z}\\\\p{C}]+)(-@)?(\\\\d+|)-(\\\\d{4}\\\\d{2}\\\\d{2}\\\\d{2})'), -10, 10), '%Y%m%d%H'))";
        final Field<Long> logtimeField = DSL.field("logtime", Long.class);
        return DSL.field(dateFromPathRegex, Long.class, JOURNALDB.LOGFILE.PATH).as(logtimeField);
    }

    public List<Row> resultRowList() {
        hostMappingTable.createIfNotExists();

        if (LOGGER.isDebugEnabled()) {
            final Explain explain = ctx.explain(selectFlatQueryStep());
            LOGGER.debug("Explain flat query <{}>", explain);
        }

        final List<Row> rowList;
        try (
                final SelectOnConditionStep<Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>> selectStep = selectFlatQueryStep()
        ) {

            final Result<Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>> result = selectStep
                    .fetch();
            rowList = new ArrayList<>(result.size());

            for (
                final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record : result
            ) {
                final Row row = new MetaRow(record);
                rowList.add(row);
            }
        }

        return rowList;
    }

    private SelectOnConditionStep<Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>> selectFlatQueryStep() {
        final Field<ULong> dayQueryIdField = rangeIdTable.field("id", ULong.class);
        return ctx
                .select(
                        JOURNALDB.LOGFILE.ID, JOURNALDB.LOGFILE.LOGDATE, JOURNALDB.LOGFILE.EXPIRATION,
                        JOURNALDB.BUCKET.NAME.as("bucket"), JOURNALDB.LOGFILE.PATH, JOURNALDB.LOGFILE.OBJECT_KEY_HASH, JOURNALDB.HOST.NAME.as("host"), JOURNALDB.LOGFILE.ORIGINAL_FILENAME, JOURNALDB.LOGFILE.ARCHIVED, JOURNALDB.LOGFILE.FILE_SIZE, JOURNALDB.METADATA_VALUE.VALUE.as("meta"), // this expects that each logfile has exactly 1 value
                        JOURNALDB.LOGFILE.SHA256_CHECKSUM, JOURNALDB.LOGFILE.ARCHIVE_ETAG, JOURNALDB.LOGFILE.LOGTAG,
                        JOURNALDB.SOURCE_SYSTEM.NAME.as("source_system"), JOURNALDB.CATEGORY.NAME.as("category"), JOURNALDB.LOGFILE.UNCOMPRESSED_FILE_SIZE, STREAMDB.STREAM.ID.as("stream_id"), // row key id
                        STREAMDB.STREAM.STREAM_, STREAMDB.STREAM.DIRECTORY, logTimeFunctionField()
                )
                .from(rangeIdTable.asTable())
                .straightJoin(JOURNALDB.LOGFILE)
                .on(JOURNALDB.LOGFILE.ID.eq(dayQueryIdField))
                .join(JOURNALDB.HOST)
                .on(JOURNALDB.LOGFILE.HOST_ID.eq(JOURNALDB.HOST.ID))
                // join host mapping temp table
                .join(hostMappingTable.table())
                .on(JOURNALDB.HOST.ID.eq(hostMappingTable.hostIdField()))
                .join(JOURNALDB.BUCKET)
                .on(JOURNALDB.LOGFILE.BUCKET_ID.eq(JOURNALDB.BUCKET.ID))
                .join(JOURNALDB.SOURCE_SYSTEM)
                .on(JOURNALDB.LOGFILE.SOURCE_SYSTEM_ID.eq(JOURNALDB.SOURCE_SYSTEM.ID))
                .join(JOURNALDB.CATEGORY)
                .on(JOURNALDB.LOGFILE.CATEGORY_ID.eq(JOURNALDB.CATEGORY.ID))
                .join(JOURNALDB.METADATA_VALUE)
                .on(JOURNALDB.LOGFILE.ID.eq(JOURNALDB.METADATA_VALUE.LOGFILE_ID))
                .join(STREAMDB.LOG_GROUP)
                .on(hostMappingTable.groupIdField().eq(STREAMDB.LOG_GROUP.ID))
                .join(STREAMDB.STREAM)
                .on(STREAMDB.LOG_GROUP.ID.eq(STREAMDB.STREAM.GID).and(JOURNALDB.LOGFILE.LOGTAG.eq(STREAMDB.STREAM.TAG)));
    }
}
