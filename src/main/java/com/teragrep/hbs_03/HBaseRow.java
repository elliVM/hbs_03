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
import org.jooq.Record18;
import org.jooq.types.ULong;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class HBaseRow implements Row {

    private final Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String> record;
    private final RowKey rowKey;

    public HBaseRow(
            final Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String> record
    ) {
        this(record, new RowKey(record.value2(), record.value18()));
    }

    public HBaseRow(
            final Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String> record,
            RowKey rowKey
    ) {
        this.record = record;
        this.rowKey = rowKey;
    }

    /**
     * Returns Record18 as a Put object ready to be directly put into a HBase table
     *
     * @return Put - a put object with Record18 values
     */
    public Put put() {
        final Map<String, Object> map = new HashMap<>();

        // add values from the record and shorter column names for qualifier
        map.put("epoch", record.field2().get(record)); // epoch
        map.put("exp", record.field3().get(record)); // expiration
        map.put("pth", record.field4().get(record)); // path
        map.put("nm", record.field5().get(record)); // name
        map.put("achvd", record.field6().get(record)); // archived
        map.put("chksum", record.field7().get(record)); // checksum
        map.put("etag", record.field8().get(record)); // etag
        map.put("ltag", record.field9().get(record)); // log tag
        map.put("sz", record.field10().get(record)); // size
        map.put("meta", record.field11().get(record)); // meta
        map.put("src", record.field12().get(record)); // source
        map.put("ctg", record.field13().get(record)); // category
        map.put("bkt", record.field14().get(record)); // bucket
        map.put("host", record.field15().get(record)); // host
        map.put("stag", record.field16().get(record)); // stream tag
        map.put("lgrp", record.field17().get(record)); // log group
        map.put("dir", record.field18().get(record)); // directory

        final Put put = new Put(rowKey.bytes(), true);
        final byte[] familyBytes = Bytes.toBytes("meta");

        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            byte[] qualifierBytes = Bytes.toBytes(entry.getKey());
            byte[] valueBytes = objectBytes(entry.getValue());
            put.addColumn(familyBytes, qualifierBytes, valueBytes);
        }

        return put;
    }

    @Override
    public String id() {
        return rowKey.value();
    }

    private byte[] objectBytes(final Object value) {
        final byte[] bytes;
        if (value instanceof String) {
            bytes = Bytes.toBytes((String) value);
        }
        else if (value instanceof Integer) {
            bytes = Bytes.toBytes((Integer) value);
        }
        else if (value instanceof ULong) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((ULong) value).longValue()).array();
        }
        else if (value instanceof Date) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((Date) value).getTime()).array();
        }
        else if (value instanceof Timestamp) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((Timestamp) value).getTime()).array();
        }
        else if (value instanceof JSON) {
            bytes = Bytes.toBytes(value.toString());
        }
        else {
            throw new IllegalArgumentException(
                    "Adding Object type <" + value.getClass().getName() + "> to HBase is not supported"
            );
        }
        return bytes;
    }
}
