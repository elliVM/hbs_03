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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jooq.JSON;
import org.jooq.Record20;
import org.jooq.Record21;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.jooq.types.UShort;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class MetaRow implements Row {

    private final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record;
    private final MetaRowKey metaRowKey;

    public MetaRow(
            final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record
    ) {
        this(record, new MetaRowKey(record.value18().longValue(), record.value21(), record.value1().longValue()));
    }

    public MetaRow(
            final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record,
            MetaRowKey metaRowKey
    ) {
        this.record = record;
        this.metaRowKey = metaRowKey;
    }

    public Put put() {
        final Map<String, Object> map = new HashMap<>();

        // add values from the record and shorter column names for qualifier
        map.put("i", record.field1().get(record));    // log file ID
        map.put("ld", record.field2().get(record));   // log date
        map.put("e", record.field3().get(record));    // expiration
        map.put("b", record.field4().get(record));    // bucket name
        map.put("p", record.field5().get(record));    // log file path
        map.put("okh", record.field6().get(record));  // object key hash
        map.put("h", record.field7().get(record));    // host name
        map.put("of", record.field8().get(record));   // original filename
        map.put("a", record.field9().get(record));    // archived
        map.put("fs", record.field10().get(record));  // file size
        map.put("m", record.field11().get(record));  // meta value
        map.put("chk", record.field12().get(record)); // sha256 checksum
        map.put("et", record.field13().get(record));  // archive ETag
        map.put("lt", record.field14().get(record));  // log tag
        map.put("src", record.field15().get(record)); // source system name
        map.put("c", record.field16().get(record));   // category name
        map.put("ufs", record.field17().get(record)); // uncompressed file size
        map.put("sid", record.field18().get(record)); // stream ID
        map.put("s", record.field19().get(record));   // stream
        map.put("d", record.field20().get(record));   // stream directory
        map.put("t", record.field21().get(record));   // logtime

        final Put put = new Put(metaRowKey.bytes(), true);
        final byte[] familyBytes = Bytes.toBytes("meta");
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            final byte[] qualifierBytes = Bytes.toBytes(key);
            final byte[] valueBytes = objectBytes(value);
            put.addColumn(familyBytes, qualifierBytes, valueBytes);
        }

        return put;
    }

    @Override
    public String id() {
        return metaRowKey.toString();
    }

    private byte[] objectBytes(final Object value) {
        final byte[] bytes;
        if (value == null) { // empty array if null
            bytes = new byte[0];
        } else if (value instanceof String) {
            bytes = Bytes.toBytes((String) value);
        } else if (value instanceof Integer) {
            bytes = Bytes.toBytes((Integer) value);
        } else if (value instanceof Long) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong((Long) value).array();
        } else if (value instanceof UShort) {
            bytes = Bytes.toBytes(((UShort) value).intValue());
        } else if (value instanceof UInteger) {
            bytes = Bytes.toBytes(((UInteger) value).intValue());
        } else if (value instanceof ULong) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((ULong) value).longValue()).array();
        } else if (value instanceof Date) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((Date) value).getTime()).array();
        } else if (value instanceof Timestamp) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((Timestamp) value).getTime()).array();
        } else if (value instanceof JSON) {
            bytes = Bytes.toBytes(value.toString());
        } else {
            throw new IllegalArgumentException(
                    "Adding object <" + value + "> with type <" + value.getClass().getName() + "> to HBase is not supported"
            );
        }
        return bytes;
    }
}
