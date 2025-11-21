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

import org.jooq.Record21;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class ValidRecordImplTest {

    @Test
    public void testId() {
        final ValidRecord validRecord = new ValidRecordImpl(createRecord(1000L, 10000L, 10L));
        final ULong id = validRecord.id();
        Assertions.assertEquals(10L, id.longValue());
    }

    @Test
    public void testRowKey() {
        final ValidRecord validRecord = new ValidRecordImpl(createRecord(485L, 17503324L, 15L));
        final MetaRowKey metaRowKey = validRecord.rowKey();
        final MetaRowKey expected = new MetaRowKey(485L, 17503324L, 15L);
        Assertions.assertEquals(expected, metaRowKey);
    }

    private Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String> createRecord(
            final long streamId,
            final long epoch,
            final long id
    ) {
        return DSL
                .using(SQLDialect.MARIADB)
                .newRecord(DSL.field("id", ULong.class), DSL.field("logtime", ULong.class), DSL.field("epoch_expires", ULong.class), DSL.field("epoch_archived", ULong.class), DSL.field("bucket", String.class), DSL.field("path", String.class), DSL.field("hash", String.class), DSL.field("host", String.class), DSL.field("file_name", String.class), DSL.field("file_size", ULong.class), DSL.field("meta", String.class), DSL.field("checksum", String.class), DSL.field("etag", String.class), DSL.field("logtag", String.class), DSL.field("source_system", String.class), DSL.field("category", String.class), DSL.field("uncompressed_filesize", ULong.class), DSL.field("ci", String.class), DSL.field("stream_id", UInteger.class), DSL.field("stream", String.class), DSL.field("directory", String.class)).values(ULong.valueOf(id), // id
                        ULong.valueOf(epoch), // logtime
                        ULong.valueOf(epoch + 84000), // epoch_expires
                        ULong.valueOf(epoch + 3600), // epoch_archived
                        "bucket", // bucket
                        "path/to/log", // path
                        "hash", // hash
                        "host", // host
                        "file.log", // file_name
                        ULong.valueOf(1000L), // file_size
                        "meta", // meta
                        "checksum", // checksum
                        "etag", // etag
                        "logtag", // logtag
                        "source_system", // source_system
                        "category", // category
                        ULong.valueOf(5000L), // uncompressed_filesize
                        "ci_value", // ci
                        UInteger.valueOf(streamId), // stream_id
                        "stream_name", // stream
                        "directory_name" // directory
                );
    }
}
