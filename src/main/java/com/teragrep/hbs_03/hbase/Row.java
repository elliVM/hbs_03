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
package com.teragrep.hbs_03.hbase;

import com.teragrep.hbs_03.hbase.binary.Binary;
import org.apache.hadoop.hbase.client.Put;
import org.jooq.Record21;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public interface Row {

    /**
     * Row values as a Put object that can be inserted into the logfile table
     */
    public abstract Put put();

    /**
     * Row key that the row will have in HBase
     */
    public abstract Binary rowKey();

    /**
     * unique identifier for the row (row key, SQL id)
     */
    public abstract ULong id();

    /**
     * Fake Row for testing
     */
    public static final class FakeRow implements Row {

        private final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record;

        FakeRow() {
            this("Bucket name");
        }

        /**
         * Default values stream id=123, logtime=2010-10-01, logfile id = 123456789
         */
        FakeRow(final String bucket) {
            this(UInteger.valueOf(123), Date.valueOf(LocalDate.of(2010, 10, 1)), ULong.valueOf(123456789), bucket);
        }

        FakeRow(final UInteger streamId, final Date logDate, final ULong logFileId, final String bucket) {
            this(
                    DSL.using(SQLDialect.MYSQL).newRecord(DSL.field("id", ULong.class), DSL.field("logdate", Date.class), DSL.field("expiration", Date.class), DSL.field("bucket", String.class), DSL.field("path", String.class), DSL.field("hash", String.class), DSL.field("host", String.class), DSL.field("file_name", String.class), DSL.field("archived", Timestamp.class), DSL.field("file_size", ULong.class), DSL.field("meta", String.class), DSL.field("checksum", String.class), DSL.field("etag", String.class), DSL.field("logtag", String.class), DSL.field("source_system", String.class), DSL.field("category", String.class), DSL.field("uncompressed_filesize", ULong.class), DSL.field("stream_id", UInteger.class), DSL.field("stream", String.class), DSL.field("directory", String.class), DSL.field("logtime", Long.class)).values(logFileId, logDate, Date.valueOf(LocalDate.of(2110, 10, 1)), bucket, String.format("%s/%s-%s/110000-sc-99-99-10-10/afe23b85-io/io-%s%s%s23.log.gz", logDate.toLocalDate().getYear(), logDate.toLocalDate().getMonthValue(), logDate.toLocalDate().getDayOfMonth(), logDate.toLocalDate().getYear(), logDate.toLocalDate().getMonthValue(), logDate.toLocalDate().getDayOfMonth()), "key_hash", "host", "original_name", Timestamp.valueOf("2010-10-01 10:00:00"), ULong.valueOf(1000L), "metadata_value", "check_sum", "ARCHIVE_ETAG", "LOGTAG", "source_system", "category", ULong.valueOf(100000L), streamId, "stream", "directory", logDate.getTime())
            );
        }

        private FakeRow(
                final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record
        ) {
            this.record = record;
        }

        @Override
        public Put put() {
            return new MetaRow(record).put();
        }

        @Override
        public Binary rowKey() {
            return new MetaRow(record).rowKey();
        }

        @Override
        public ULong id() {
            return new MetaRow(record).id();
        }
    }
}
