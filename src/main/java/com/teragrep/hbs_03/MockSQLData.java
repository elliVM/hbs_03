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

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSON;
import org.jooq.Record18;
import org.jooq.Record20;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;
import static com.teragrep.hbs_03.jooq.generated.streamdb.Streamdb.STREAMDB;

public class MockSQLData implements MockDataProvider {

    final DSLContext ctx = DSL.using(SQLDialect.MYSQL);

    @Override
    public MockResult[] execute(final MockExecuteContext ctx) {
        final MockResult[] mock;
        final String sql = ctx.sql();
        if (sql.toUpperCase().startsWith("ONE")) {
            mock = generateResult(1);
        } else if (sql.toUpperCase().startsWith("ROWS_")) {
            final int customRows = Integer.parseInt(sql.substring("ROWS_".length()));
            mock = generateResult(customRows);
        } else {
            mock = new MockResult[]{
                    new MockResult(0, this.ctx.newResult())
            };
        }
        return mock;
    }

    // generates always same results to the set range
    MockResult[] generateResult(int numOfResults) {
        final Field<Long> logtimeFunction = DSL
                .field(
                        "UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR({0},'^\\\\d{4}\\\\/\\\\d{2}-\\\\d{2}\\\\/[\\\\w\\\\.-]+\\\\/([^\\\\p{Z}\\\\p{C}]+?)\\\\/([^\\\\p{Z}\\\\p{C}]+)(-@)?(\\\\d+|)-(\\\\d{4}\\\\d{2}\\\\d{2}\\\\d{2})'), -10, 10), '%Y%m%d%H'))",
                        Long.class, JOURNALDB.LOGFILE.PATH
                );
        final Field<Long> logtimeField = DSL.field("logtime", Long.class);

        final Result<Record20<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, ULong, UInteger, String, String, Long>> result =
                ctx.newResult(
                        JOURNALDB.LOGFILE.ID.as("id"),
                        JOURNALDB.LOGFILE.LOGDATE.as("logdate"),
                        JOURNALDB.LOGFILE.EXPIRATION.as("expiration"),
                        JOURNALDB.BUCKET.NAME.as("bucket"),
                        JOURNALDB.LOGFILE.PATH.as("path"),
                        JOURNALDB.LOGFILE.OBJECT_KEY_HASH.as("hash"),
                        JOURNALDB.HOST.NAME.as("host"),
                        JOURNALDB.LOGFILE.ORIGINAL_FILENAME.as("file_name"),
                        JOURNALDB.LOGFILE.ARCHIVED.as("archived"),
                        JOURNALDB.LOGFILE.FILE_SIZE.as("file_size"),
                        JOURNALDB.LOGFILE.SHA256_CHECKSUM.as("checksum"),
                        JOURNALDB.LOGFILE.ARCHIVE_ETAG.as("etag"),
                        JOURNALDB.LOGFILE.LOGTAG.as("logtag"),
                        JOURNALDB.SOURCE_SYSTEM.NAME.as("source_system"),
                        JOURNALDB.CATEGORY.NAME.as("category"),
                        JOURNALDB.LOGFILE.UNCOMPRESSED_FILE_SIZE.as("uncompressed_filesize"),
                        STREAMDB.STREAM.ID.as("stream_id"),
                        STREAMDB.STREAM.STREAM_.as("stream"),
                        STREAMDB.STREAM.DIRECTORY.as("directory"),
                        logtimeFunction.as(logtimeField)
                );

        for (long l = 1; l <= numOfResults; l++) {
            int year = 2010;
            int month = 10;
            // day between 1-28
            final int day = (l % 28) == 0 ? 28 : (int) l % 28;
            final Timestamp timestamp = Timestamp.valueOf(String.format("%s-%s-%s 10:00:00", year, month, day));
            final LocalDate local = LocalDate.of(year, month, day);
            final LocalDate localExpiration = LocalDate.of(year + 100, month, day);
            final Date date = Date.valueOf(local);
            final Date expireDate = Date.valueOf(localExpiration);
            // archived date as epoch + 1000
            final long epoch = date.getTime();
            final String path = String.format("%s/%s-%s/110000-sc-99-99-10-10/afe23b85-io/io-%s%s%s23.log.gz", year, month, day, year, month, day);
            result.add(ctx.newRecord(
                    JOURNALDB.LOGFILE.ID.as("id"),
                    JOURNALDB.LOGFILE.LOGDATE.as("logdate"),
                    JOURNALDB.LOGFILE.EXPIRATION.as("expiration"),
                    JOURNALDB.BUCKET.NAME.as("bucket"),
                    JOURNALDB.LOGFILE.PATH.as("path"),
                    JOURNALDB.LOGFILE.OBJECT_KEY_HASH.as("hash"),
                    JOURNALDB.HOST.NAME.as("host"),
                    JOURNALDB.LOGFILE.ORIGINAL_FILENAME.as("file_name"),
                    JOURNALDB.LOGFILE.ARCHIVED.as("archived"),
                    JOURNALDB.LOGFILE.FILE_SIZE.as("file_size"),
                    JOURNALDB.LOGFILE.SHA256_CHECKSUM.as("checksum"),
                    JOURNALDB.LOGFILE.ARCHIVE_ETAG.as("etag"),
                    JOURNALDB.LOGFILE.LOGTAG.as("logtag"),
                    JOURNALDB.SOURCE_SYSTEM.NAME.as("source_system"),
                    JOURNALDB.CATEGORY.NAME.as("category"),
                    JOURNALDB.LOGFILE.UNCOMPRESSED_FILE_SIZE.as("uncompressed_filesize"),
                    STREAMDB.STREAM.ID.as("stream_id"),
                    STREAMDB.STREAM.STREAM_.as("stream"),
                    STREAMDB.STREAM.DIRECTORY.as("directory"),
                    logtimeFunction.as(logtimeField)
            ).values(
                    ULong.valueOf(l),
                    date,
                    expireDate,
                    "BUCKET name",
                    path,
                    "key_hash",
                    "host",
                    "original_name",
                    timestamp,
                    ULong.valueOf(1000L),
                    "check_sum",
                    "ARCHIVE_ETAG",
                    "LOGTAG",
                    "source_system",
                    "category",
                    ULong.valueOf(100000L),
                    UInteger.valueOf(l + 1000),
                    "stream",
                    "directory",
                    epoch
            ));
        }

        return new MockResult[]{
                new MockResult(numOfResults, result)
        };
    }
}
