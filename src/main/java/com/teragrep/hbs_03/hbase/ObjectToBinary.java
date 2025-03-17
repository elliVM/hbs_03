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

import com.teragrep.hbs_03.HbsRuntimeException;
import com.teragrep.hbs_03.hbase.binary.Binary;
import org.apache.hadoop.hbase.util.Bytes;
import org.jooq.JSON;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.jooq.types.UShort;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Represents an encapsulated object as bytes
 */
public final class ObjectToBinary implements Binary {

    private final Object object;

    public ObjectToBinary(final Object object) {
        this.object = object;
    }

    public byte[] bytes() {
        final byte[] bytes;
        if (object == null) { // empty bytes represents a null value in hbase
            bytes = new byte[0];
        }
        else if (object instanceof String) {
            bytes = Bytes.toBytes((String) object);
        }
        else if (object instanceof Integer) {
            bytes = Bytes.toBytes((Integer) object);
        }
        else if (object instanceof Long) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong((Long) object).array();
        }
        else if (object instanceof UShort) {
            bytes = Bytes.toBytes(((UShort) object).intValue());
        }
        else if (object instanceof UInteger) {
            bytes = Bytes.toBytes(((UInteger) object).intValue());
        }
        else if (object instanceof ULong) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((ULong) object).longValue()).array();
        }
        else if (object instanceof Date) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((Date) object).getTime()).array();
        }
        else if (object instanceof Timestamp) {
            bytes = ByteBuffer.allocate(Long.BYTES).putLong(((Timestamp) object).getTime()).array();
        }
        else if (object instanceof JSON) {
            bytes = Bytes.toBytes(object.toString());
        }
        else {
            throw new HbsRuntimeException(
                    "Error getting binary form",
                    new IllegalArgumentException(
                            "Binary support for <" + object + "> with object type of <" + object.getClass().getName()
                                    + "> is not supported"
                    )
            );
        }
        return bytes;
    }
}
