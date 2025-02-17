package com.teragrep.hbs_03;

import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record20;
import org.jooq.Record21;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;

import java.sql.Date;
import java.sql.Timestamp;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;
import static com.teragrep.hbs_03.jooq.generated.streamdb.Streamdb.STREAMDB;

/**
 * Queries all values used by HBase row
 */
public class LogfileTableFlatDayQuery {
    final DSLContext ctx;
    final LogfileTableDayQuery logfileTableDayQuery;
    final int fetchSize;

    public LogfileTableFlatDayQuery(final DSLContext ctx, final Date day, final int fetchSize) {
        this(ctx, new LogfileTableDayQuery(ctx, day), fetchSize);
    }

    public LogfileTableFlatDayQuery(final DSLContext ctx, final LogfileTableDayQuery logfileTableDayQuery, final int fetchSize) {
        this.ctx = ctx;
        this.logfileTableDayQuery = logfileTableDayQuery;
        this.fetchSize = fetchSize;
    }

    public Field<Long> logTimeFunctionField() {
        final String dateFromPathRegex = "UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR({0},'^\\\\d{4}\\\\/\\\\d{2}-\\\\d{2}\\\\/[\\\\w\\\\.-]+\\\\/([^\\\\p{Z}\\\\p{C}]+?)\\\\/([^\\\\p{Z}\\\\p{C}]+)(-@)?(\\\\d+|)-(\\\\d{4}\\\\d{2}\\\\d{2}\\\\d{2})'), -10, 10), '%Y%m%d%H'))";
        final Field<Long> logtimeField = DSL.field("logtime", Long.class);
        return DSL
                .field(dateFromPathRegex, Long.class, JOURNALDB.LOGFILE.PATH).as(logtimeField);
    }

    public Cursor<Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>> asCursor() {
        // TODO: all joined rows might not have not have streamdb information
        return ctx.select(
                        JOURNALDB.LOGFILE.ID,
                        JOURNALDB.LOGFILE.LOGDATE,
                        JOURNALDB.LOGFILE.EXPIRATION,
                        JOURNALDB.BUCKET.NAME.as("bucket"),
                        JOURNALDB.LOGFILE.PATH,
                        JOURNALDB.LOGFILE.OBJECT_KEY_HASH,
                        JOURNALDB.HOST.NAME.as("host"),
                        JOURNALDB.LOGFILE.ORIGINAL_FILENAME,
                        JOURNALDB.LOGFILE.ARCHIVED,
                        JOURNALDB.LOGFILE.FILE_SIZE,
                        JOURNALDB.METADATA_VALUE.VALUE.as("meta"),
                        JOURNALDB.LOGFILE.SHA256_CHECKSUM,
                        JOURNALDB.LOGFILE.ARCHIVE_ETAG,
                        JOURNALDB.LOGFILE.LOGTAG,
                        JOURNALDB.SOURCE_SYSTEM.NAME.as("source_system"),
                        JOURNALDB.CATEGORY.NAME.as("category"),
                        JOURNALDB.LOGFILE.UNCOMPRESSED_FILE_SIZE,
                        STREAMDB.STREAM.ID.as("stream_id"), // row key id
                        STREAMDB.STREAM.STREAM_,
                        STREAMDB.STREAM.DIRECTORY,
                        logTimeFunctionField()
                )
                .from(logfileTableDayQuery.asTable())
                .join(JOURNALDB.LOGFILE)
                .on(JOURNALDB.LOGFILE.ID.eq(logfileTableDayQuery.idField()))
                .join(JOURNALDB.HOST)
                .on(JOURNALDB.LOGFILE.HOST_ID.eq(JOURNALDB.HOST.ID))
                .join(JOURNALDB.BUCKET)
                .on(JOURNALDB.LOGFILE.BUCKET_ID.eq(JOURNALDB.BUCKET.ID))
                .join(JOURNALDB.SOURCE_SYSTEM)
                .on(JOURNALDB.LOGFILE.SOURCE_SYSTEM_ID.eq(JOURNALDB.SOURCE_SYSTEM.ID))
                .join(JOURNALDB.CATEGORY)
                .on(JOURNALDB.LOGFILE.CATEGORY_ID.eq(JOURNALDB.CATEGORY.ID))
                .join(JOURNALDB.METADATA_VALUE)
                .on(JOURNALDB.LOGFILE.ID.eq(JOURNALDB.METADATA_VALUE.LOGFILE_ID))
                .join(STREAMDB.HOST)
                .on(JOURNALDB.HOST.NAME.eq(STREAMDB.HOST.NAME))
                .join(STREAMDB.LOG_GROUP)
                .on(STREAMDB.HOST.GID.eq(STREAMDB.LOG_GROUP.ID))
                .join(STREAMDB.STREAM)
                .on(STREAMDB.LOG_GROUP.ID.eq(STREAMDB.STREAM.GID).and(JOURNALDB.LOGFILE.LOGTAG.eq(STREAMDB.STREAM.TAG)))
                .fetchSize(fetchSize) // set fetch size for the JDBC driver
                .fetchLazy();
    }
}
