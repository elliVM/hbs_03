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
import com.teragrep.hbs_03.hbase.binary.BinaryOfString;
import com.teragrep.hbs_03.hbase.binary.BinaryOfUInteger;
import com.teragrep.hbs_03.hbase.binary.BinaryOfULong;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.jooq.Record21;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;

/** Represents a row for meta-column family */
public final class MetaRow implements Row {

    private final ValidRecord validRecord;

    public MetaRow(
            final Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String> record
    ) {
        this(new ValidRecord(record));
    }

    public MetaRow(final ValidRecord validRecord) {
        this.validRecord = validRecord;
    }

    public Put put() {

        final Put put = new Put(validRecord.rowKey().bytes(), true);
        final byte[] familyBytes = Bytes.toBytes("meta");
        Record21<ULong, ULong, ULong, ULong, String, String, String, String, String, ULong, String, String, String, String, String, String, ULong, String, UInteger, String, String> record = validRecord.record;

        // add values from the record and shorter column names for qualifier
        put.addColumn(familyBytes, Bytes.toBytes("i"), new BinaryOfULong(record.value1()).bytes()); // log file ID
        put.addColumn(familyBytes, Bytes.toBytes("t"), new BinaryOfULong(record.value2()).bytes()); // logdate epoch
        put.addColumn(familyBytes, Bytes.toBytes("e"), new BinaryOfULong(record.value3()).bytes()); // expiration epoch
        put.addColumn(familyBytes, Bytes.toBytes("a"), new BinaryOfULong(record.value4()).bytes()); // archived epoch
        put.addColumn(familyBytes, Bytes.toBytes("b"), new BinaryOfString(record.value5()).bytes()); // bucket name
        put.addColumn(familyBytes, Bytes.toBytes("p"), new BinaryOfString(record.value6()).bytes()); // log file path
        put.addColumn(familyBytes, Bytes.toBytes("okh"), new BinaryOfString(record.value7()).bytes()); // object key hash
        put.addColumn(familyBytes, Bytes.toBytes("h"), new BinaryOfString(record.value8()).bytes()); // host name
        put.addColumn(familyBytes, Bytes.toBytes("of"), new BinaryOfString(record.value9()).bytes()); // original filename
        put.addColumn(familyBytes, Bytes.toBytes("fs"), new BinaryOfULong(record.value10()).bytes()); // file size
        put.addColumn(familyBytes, Bytes.toBytes("m"), new BinaryOfString(record.value11()).bytes()); // meta value
        put.addColumn(familyBytes, Bytes.toBytes("chk"), new BinaryOfString(record.value12()).bytes()); // sha256 checksum
        put.addColumn(familyBytes, Bytes.toBytes("et"), new BinaryOfString(record.value13()).bytes()); // archive ETag
        put.addColumn(familyBytes, Bytes.toBytes("lt"), new BinaryOfString(record.value14()).bytes()); // log tag
        put.addColumn(familyBytes, Bytes.toBytes("src"), new BinaryOfString(record.value15()).bytes()); // source system name
        put.addColumn(familyBytes, Bytes.toBytes("c"), new BinaryOfString(record.value16()).bytes()); // category name
        put.addColumn(familyBytes, Bytes.toBytes("ufs"), new BinaryOfULong(record.value17()).bytes()); // uncompressed file size
        put.addColumn(familyBytes, Bytes.toBytes("ci"), new BinaryOfString(record.value18()).bytes()); // ci
        put.addColumn(familyBytes, Bytes.toBytes("sid"), new BinaryOfUInteger(record.value19()).bytes()); // stream ID
        put.addColumn(familyBytes, Bytes.toBytes("s"), new BinaryOfString(record.value20()).bytes()); // stream
        put.addColumn(familyBytes, Bytes.toBytes("d"), new BinaryOfString(record.value21()).bytes()); // stream directory

        return put;
    }

    public Binary rowKey() {
        return validRecord.rowKey();
    }

    @Override
    public ULong id() {
        return validRecord.id();
    }

}
