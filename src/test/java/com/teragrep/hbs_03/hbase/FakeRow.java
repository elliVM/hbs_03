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

import java.time.Instant;
import java.time.ZoneId;

public final class FakeRow implements Row {

    final Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String> record;

    public FakeRow() {
        this("Bucket name");
    }

    /**
     * Default values stream id=123, logtime=2010-10-01, logfile id = 123456789
     */
    public FakeRow(final String bucket) {
        this(UInteger.valueOf(123), 1285894800L, ULong.valueOf(123456789), bucket);
    }

    public FakeRow(final long customId) {
        this(UInteger.valueOf(123), 1285894800L, ULong.valueOf(customId), "bucket");
    }

    public FakeRow(final UInteger streamId, final long epoch, final ULong logFileId, final String bucket) {
        this(
                DSL.using(SQLDialect.MYSQL).newRecord(DSL.field("id", ULong.class), DSL.field("logtime", ULong.class), DSL.field("epoch_archived", ULong.class), DSL.field("epoch_expires", ULong.class), DSL.field("bucket", String.class), DSL.field("path", String.class), DSL.field("hash", String.class), DSL.field("host", String.class), DSL.field("file_name", String.class), DSL.field("file_size", ULong.class), DSL.field("meta", String.class), DSL.field("checksum", String.class), DSL.field("etag", String.class), DSL.field("logtag", String.class), DSL.field("source_system", String.class), DSL.field("category", String.class), DSL.field("uncompressed_filesize", ULong.class), DSL.field("ci", String.class), DSL.field("stream_id", UInteger.class), DSL.field("stream", String.class), DSL.field("directory", String.class)).values(logFileId, ULong.valueOf(epoch), ULong.valueOf(epoch + 3600), ULong.valueOf(epoch + 86400), bucket, String.format("%s/%s-%s/110000-sc-99-99-10-10/afe23b85-io/io-%s%s%s23.log.gz", Instant.ofEpochSecond(epoch).atZone(ZoneId.of("UTC")).getYear(), Instant.ofEpochSecond(epoch).atZone(ZoneId.of("UTC")).getMonthValue(), Instant.ofEpochSecond(epoch).atZone(ZoneId.of("UTC")).getDayOfMonth(), Instant.ofEpochSecond(epoch).atZone(ZoneId.of("UTC")).getYear(), Instant.ofEpochSecond(epoch).atZone(ZoneId.of("UTC")).getMonthValue(), Instant.ofEpochSecond(epoch).atZone(ZoneId.of("UTC")).getDayOfMonth()), "key_hash", "host", "original_name", ULong.valueOf(1000L), "metadata_value", "check_sum", "ARCHIVE_ETAG", "LOGTAG", "source_system", "category", ULong.valueOf(100000L), "ci", streamId, "stream", "directory")
        );
    }

    private FakeRow(
            final Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String> record
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
