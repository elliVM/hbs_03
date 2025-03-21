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
import com.teragrep.hbs_03.hbase.binary.BinaryOfDate;
import com.teragrep.hbs_03.hbase.binary.BinaryOfLong;
import com.teragrep.hbs_03.hbase.binary.BinaryOfString;
import com.teragrep.hbs_03.hbase.binary.BinaryOfTimestamp;
import com.teragrep.hbs_03.hbase.binary.BinaryOfUInteger;
import com.teragrep.hbs_03.hbase.binary.BinaryOfULong;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jooq.Record21;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;

import java.sql.Date;
import java.sql.Timestamp;

/** Represents a row for meta-column family */
public final class MetaRow implements Row {

    private final ValidRecord21 validRecord21;

    public MetaRow(
            final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record
    ) {
        this(new ValidRecord21(record));
    }

    public MetaRow(final ValidRecord21 validRecord21) {
        this.validRecord21 = validRecord21;
    }

    public Put put() {

        final Put put = new Put(validRecord21.rowKey().bytes(), true);
        final byte[] familyBytes = Bytes.toBytes("meta");
        final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record = validRecord21.record21;

        // add values from the record and shorter column names for qualifier
        put.addColumn(familyBytes, Bytes.toBytes("i"), new BinaryOfULong(record.field1().get(record)).bytes()); // log file ID
        put.addColumn(familyBytes, Bytes.toBytes("ld"), new BinaryOfDate(record.field2().get(record)).bytes()); // log date
        put.addColumn(familyBytes, Bytes.toBytes("e"), new BinaryOfDate(record.field3().get(record)).bytes()); // expiration
        put.addColumn(familyBytes, Bytes.toBytes("b"), new BinaryOfString(record.field4().get(record)).bytes()); // bucket name
        put.addColumn(familyBytes, Bytes.toBytes("p"), new BinaryOfString(record.field5().get(record)).bytes()); // log file path
        put.addColumn(familyBytes, Bytes.toBytes("okh"), new BinaryOfString(record.field6().get(record)).bytes()); // object key hash
        put.addColumn(familyBytes, Bytes.toBytes("h"), new BinaryOfString(record.field7().get(record)).bytes()); // host name
        put.addColumn(familyBytes, Bytes.toBytes("of"), new BinaryOfString(record.field8().get(record)).bytes()); // original filename
        put.addColumn(familyBytes, Bytes.toBytes("a"), new BinaryOfTimestamp(record.field9().get(record)).bytes()); // archived
        put.addColumn(familyBytes, Bytes.toBytes("fs"), new BinaryOfULong(record.field10().get(record)).bytes()); // file size
        put.addColumn(familyBytes, Bytes.toBytes("m"), new BinaryOfString(record.field11().get(record)).bytes()); // meta value
        put.addColumn(familyBytes, Bytes.toBytes("chk"), new BinaryOfString(record.field12().get(record)).bytes()); // sha256 checksum
        put.addColumn(familyBytes, Bytes.toBytes("et"), new BinaryOfString(record.field13().get(record)).bytes()); // archive ETag
        put.addColumn(familyBytes, Bytes.toBytes("lt"), new BinaryOfString(record.field14().get(record)).bytes()); // log tag
        put.addColumn(familyBytes, Bytes.toBytes("src"), new BinaryOfString(record.field15().get(record)).bytes()); // source system name
        put.addColumn(familyBytes, Bytes.toBytes("c"), new BinaryOfString(record.field16().get(record)).bytes()); // category name
        put.addColumn(familyBytes, Bytes.toBytes("ufs"), new BinaryOfULong(record.field17().get(record)).bytes()); // uncompressed file size
        put.addColumn(familyBytes, Bytes.toBytes("sid"), new BinaryOfUInteger(record.field18().get(record)).bytes()); // stream ID
        put.addColumn(familyBytes, Bytes.toBytes("s"), new BinaryOfString(record.field19().get(record)).bytes()); // stream
        put.addColumn(familyBytes, Bytes.toBytes("d"), new BinaryOfString(record.field20().get(record)).bytes()); // stream directory
        put.addColumn(familyBytes, Bytes.toBytes("t"), new BinaryOfLong(record.field21().get(record)).bytes()); // logtime

        return put;
    }

    public Binary rowKey() {
        return validRecord21.rowKey();
    }

    @Override
    public ULong id() {
        return validRecord21.id();
    }

}
