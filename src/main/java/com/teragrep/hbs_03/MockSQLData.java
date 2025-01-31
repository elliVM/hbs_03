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
import org.jooq.JSON;
import org.jooq.Record18;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.jooq.types.ULong;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.BUCKET;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.CATEGORY;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.HOST;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.LOGFILE;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.METADATA_VALUE;
import static com.teragrep.hbs_03.jooq.generated.journaldb.Tables.SOURCE_SYSTEM;
import static com.teragrep.hbs_03.jooq.generated.streamdb.Streamdb.STREAMDB;

public class MockSQLData implements MockDataProvider {

    final DSLContext ctx = DSL.using(SQLDialect.MYSQL);

    @Override
    public MockResult[] execute(final MockExecuteContext ctx) {
        final MockResult[] mock;
        final String sql = ctx.sql();
        if (sql.toUpperCase().startsWith("ONE")) {
            mock = generateResult(1);
        }
        else if (sql.toUpperCase().startsWith("ROWS_")) {
            final int customRows = Integer.parseInt(sql.substring("ROWS_".length()));
            mock = generateResult(customRows);
        }
        else {
            mock = new MockResult[] {
                    new MockResult(0, this.ctx.newResult())
            };
        }
        return mock;
    }

    // generates always same results to the set range
    MockResult[] generateResult(int numOfResults) {
        final Result<Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>> result = ctx
                .newResult(LOGFILE.ID.as("id"), DSL.epoch(DSL.timestamp(LOGFILE.LOGDATE)).as("epoch"), LOGFILE.EXPIRATION.as("exp"), LOGFILE.PATH.as("pth"), LOGFILE.ORIGINAL_FILENAME.as("name"), DSL.timestamp(LOGFILE.ARCHIVED).as("archived"), LOGFILE.SHA256_CHECKSUM.as("checksum"), LOGFILE.ARCHIVE_ETAG.as("etag"), LOGFILE.LOGTAG.as("logtag"), LOGFILE.UNCOMPRESSED_FILE_SIZE.as("size"), DSL.jsonObjectAgg(METADATA_VALUE.VALUE_KEY.concat(":").concat(METADATA_VALUE.VALUE)).as("meta"), SOURCE_SYSTEM.NAME.as("src"), CATEGORY.NAME.as("ctg"), BUCKET.NAME.as("bckt"), HOST.NAME.as("host"), STREAMDB.STREAM.TAG.as("stream_tag"), STREAMDB.LOG_GROUP.NAME.as("log_group"), STREAMDB.STREAM.DIRECTORY.as("directory"));

        for (long l = 1; l <= numOfResults; l++) {
            // date between 1-28
            final int day = (l % 28) == 0 ? 28 : (int) l % 28;
            final Timestamp timestamp = Timestamp.valueOf(String.format("2010-10-%s 10:00:00", day));
            final LocalDate local = LocalDate.of(2010, 10, day);
            final Date date = Date.valueOf(local);
            // archived date as epoch + 1000
            final int epoch = (int) date.getTime() + 1000;
            result
                    .add(ctx.newRecord(LOGFILE.ID.as("id"), DSL.epoch(DSL.timestamp(LOGFILE.LOGDATE)).as("epoch"), LOGFILE.EXPIRATION.as("exp"), LOGFILE.PATH.as("pth"), LOGFILE.ORIGINAL_FILENAME.as("name"), DSL.timestamp(LOGFILE.ARCHIVED).as("archived"), LOGFILE.SHA256_CHECKSUM.as("checksum"), LOGFILE.ARCHIVE_ETAG.as("etag"), LOGFILE.LOGTAG.as("logtag"), LOGFILE.UNCOMPRESSED_FILE_SIZE.as("size"), DSL.jsonObjectAgg(METADATA_VALUE.VALUE_KEY.concat(":").concat(METADATA_VALUE.VALUE)).as("meta"), SOURCE_SYSTEM.NAME.as("src"), CATEGORY.NAME.as("ctg"), BUCKET.NAME.as("bckt"), HOST.NAME.as("host"), STREAMDB.STREAM.TAG.as("stream_tag"), STREAMDB.LOG_GROUP.NAME.as("log_group"), STREAMDB.STREAM.DIRECTORY.as("directory")).values(ULong.valueOf(l), epoch, date, "path", "original name", timestamp, "checksum", "etag", "log_tag", ULong.valueOf(10), JSON.valueOf("{\"key\": \"value\"}"), "source", "category", "bucket", "host", "stream_tag", "log_group", "directory"));
        }

        return new MockResult[] {
                new MockResult(numOfResults, result)
        };
    }
}
