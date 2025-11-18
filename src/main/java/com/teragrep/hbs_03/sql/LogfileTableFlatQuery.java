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
    private final HostMappingTempTable hostMappingTempTable;

    public LogfileTableFlatQuery(final DSLContext ctx, final long startId, final long endId) {
        this(ctx, new LogfileTableIdRangeQuery(ctx, startId, endId).asTable(), new HostMappingTempTable(ctx));
    }

    public LogfileTableFlatQuery(final DSLContext ctx, final LogfileTableIdRangeQuery rangeIdQuery) {
        this(ctx, rangeIdQuery.asTable(), new HostMappingTempTable(ctx));
    }

    public LogfileTableFlatQuery(
            final DSLContext ctx,
            final Table<Record1<ULong>> rangeIdTable,
            final HostMappingTempTable hostMappingTempTable
    ) {
        this.ctx = ctx;
        this.rangeIdTable = rangeIdTable;
        this.hostMappingTempTable = hostMappingTempTable;
    }

    private Field<ULong> coalescedLogtimeField() {
        final String dateFromPathRegex = "UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR({0},'^\\\\d{4}\\\\/\\\\d{2}-\\\\d{2}\\\\/[\\\\w\\\\.-]+\\\\/([^\\\\p{Z}\\\\p{C}]+?)\\\\/([^\\\\p{Z}\\\\p{C}]+)(-@)?(\\\\d+|)-(\\\\d{4}\\\\d{2}\\\\d{2}\\\\d{2})'), -10, 10), '%Y%m%d%H'))";
        final Field<ULong> extractedLogtimeField = DSL.field(dateFromPathRegex, ULong.class, JOURNALDB.LOGFILE.PATH);
        return DSL.coalesce(JOURNALDB.LOGFILE.EPOCH_HOUR, extractedLogtimeField).as("logtime");
    }

    private Field<ULong> coalescedEpochExpiresField() {
        return DSL
                .coalesce(
                        JOURNALDB.LOGFILE.EPOCH_EXPIRES,
                        DSL.field("UNIX_TIMESTAMP({0})", ULong.class, JOURNALDB.LOGFILE.EXPIRATION)
                )
                .as("epoch_expires");
    }

    private Field<ULong> coalescedEpochArchivedField() {
        return DSL
                .coalesce(
                        JOURNALDB.LOGFILE.EPOCH_ARCHIVED,
                        DSL.field("UNIX_TIMESTAMP({0})", ULong.class, JOURNALDB.LOGFILE.ARCHIVED)
                )
                .as("epoch_archived");
    }

    public List<Row> resultRowList() {
        hostMappingTempTable.createIfNotExists();

        if (LOGGER.isDebugEnabled()) {
            final Explain explain = ctx.explain(selectFlatQueryStep());
            LOGGER.debug("Explain flat query <{}>", explain);
        }

        final List<Row> rowList;
        try (
                final SelectOnConditionStep<Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String>> selectStep = selectFlatQueryStep()
        ) {

            final Result<Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String>> result = selectStep
                    .fetch();
            rowList = new ArrayList<>(result.size());

            for (
                final Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String> record : result
            ) {
                final Row row = new MetaRow(record);
                rowList.add(row);
            }
        }

        return rowList;
    }

    private SelectOnConditionStep<Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String>> selectFlatQueryStep() {
        final Field<ULong> dayQueryIdField = rangeIdTable.field("id", ULong.class);
        final SelectOnConditionStep<Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String>> selectStep = ctx
                .select(
                        JOURNALDB.LOGFILE.ID, coalescedLogtimeField(), coalescedEpochArchivedField(),
                        coalescedEpochExpiresField(), JOURNALDB.BUCKET.NAME.as("bucket"), JOURNALDB.LOGFILE.PATH, JOURNALDB.LOGFILE.OBJECT_KEY_HASH, JOURNALDB.HOST.NAME.as("host"), JOURNALDB.LOGFILE.ORIGINAL_FILENAME, JOURNALDB.LOGFILE.FILE_SIZE, JOURNALDB.METADATA_VALUE.VALUE.as("meta"), // this expects that each logfile has exactly 1 value
                        JOURNALDB.LOGFILE.SHA256_CHECKSUM, JOURNALDB.LOGFILE.ARCHIVE_ETAG, JOURNALDB.LOGTAG.LOGTAG_,
                        JOURNALDB.SOURCE_SYSTEM.NAME.as("source_system"), JOURNALDB.CATEGORY.NAME.as("category"), JOURNALDB.LOGFILE.UNCOMPRESSED_FILE_SIZE, JOURNALDB.CI.NAME, STREAMDB.STREAM.ID.as("stream_id"), STREAMDB.STREAM.STREAM_, STREAMDB.STREAM.DIRECTORY
                )
                .from(rangeIdTable)
                .straightJoin(JOURNALDB.LOGFILE)
                .on(JOURNALDB.LOGFILE.ID.eq(dayQueryIdField))
                .join(JOURNALDB.HOST)
                .on(JOURNALDB.LOGFILE.HOST_ID.eq(JOURNALDB.HOST.ID))
                // join host mapping temp table
                .join(hostMappingTempTable.table())
                .on(JOURNALDB.HOST.ID.eq(hostMappingTempTable.hostIdField()))
                .join(JOURNALDB.BUCKET)
                .on(JOURNALDB.LOGFILE.BUCKET_ID.eq(JOURNALDB.BUCKET.ID))
                .join(JOURNALDB.SOURCE_SYSTEM)
                .on(JOURNALDB.LOGFILE.SOURCE_SYSTEM_ID.eq(JOURNALDB.SOURCE_SYSTEM.ID))
                .join(JOURNALDB.CATEGORY)
                .on(JOURNALDB.LOGFILE.CATEGORY_ID.eq(JOURNALDB.CATEGORY.ID))
                .join(JOURNALDB.METADATA_VALUE)
                .on(JOURNALDB.LOGFILE.ID.eq(JOURNALDB.METADATA_VALUE.LOGFILE_ID))
                .join(JOURNALDB.LOGTAG)
                .on(JOURNALDB.LOGFILE.LOGTAG_ID.eq(JOURNALDB.LOGTAG.ID))
                .join(JOURNALDB.CI)
                .on(JOURNALDB.LOGFILE.CI_ID.eq(JOURNALDB.CI.ID))
                .join(STREAMDB.LOG_GROUP)
                .on(hostMappingTempTable.groupIdField().eq(STREAMDB.LOG_GROUP.ID))
                .join(STREAMDB.STREAM)
                .on(STREAMDB.LOG_GROUP.ID.eq(STREAMDB.STREAM.GID).and(JOURNALDB.LOGTAG.LOGTAG_.eq(STREAMDB.STREAM.TAG)));
        return selectStep;
    }
}
