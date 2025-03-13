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

import com.teragrep.hbs_03.hbase.Binary;
import com.teragrep.hbs_03.hbase.ObjectToBinary;
import org.jooq.JSON;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.jooq.types.UShort;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

public final class ObjectToBinaryTest {

    @Test
    public void testNull() {
        final byte[] nullBytes = new ObjectToBinary(null).bytes();
        Assertions.assertEquals(new byte[0].length, nullBytes.length);
    }

    @Test
    public void testString() {
        final String input = "test";
        final byte[] bytes = new ObjectToBinary(input).bytes();
        byte[] expected = input.getBytes(StandardCharsets.UTF_8);
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testIntegerMaxValue() {
        final Integer input = Integer.MAX_VALUE;
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Integer.BYTES).putInt(input).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testIntegerMinValue() {
        final Integer input = Integer.MIN_VALUE;
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Integer.BYTES).putInt(input).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testLongMaxValue() {
        final Long input = Long.MAX_VALUE;
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Long.BYTES).putLong(input).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testLongMinValue() {
        final Long input = Long.MIN_VALUE;
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Long.BYTES).putLong(input).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testULongMaxValue() {
        final ULong input = ULong.valueOf(ULong.MAX_VALUE);
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Long.BYTES).putLong(input.longValue()).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testULongMinValue() {
        final ULong input = ULong.valueOf(ULong.MIN_VALUE);
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Long.BYTES).putLong(input.longValue()).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testUShortMaxValue() {
        final UShort input = UShort.valueOf(UShort.MAX_VALUE);
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Integer.BYTES).putInt(input.intValue()).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testUShortMinValue() {
        final UShort input = UShort.valueOf(UShort.MIN_VALUE);
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Integer.BYTES).putInt(input.intValue()).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testUIntegerMaxValue() {
        final UInteger input = UInteger.valueOf(UInteger.MAX_VALUE);
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Integer.BYTES).putInt(input.intValue()).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testUIntegerMinValue() {
        final UInteger input = UInteger.valueOf(UInteger.MIN_VALUE);
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Integer.BYTES).putInt(input.intValue()).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testDate() {
        final Date input = Date.valueOf("1970-01-01");
        final byte[] bytes = new ObjectToBinary(input).bytes();
        final byte[] expected = ByteBuffer.allocate(Long.BYTES).putLong(input.getTime()).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testJSON() {
        final String jsonString = "{ \"key\": \"keyValue\", \"value\": \"valueValue\" } ";
        final JSON jooqJson = JSON.json(jsonString);
        final byte[] bytes = new ObjectToBinary(jooqJson).bytes();
        final byte[] expected = jsonString.getBytes(StandardCharsets.UTF_8);
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testTimestamp() {
        final Timestamp timestamp = new Timestamp(100000L);
        final byte[] bytes = new ObjectToBinary(timestamp).bytes();
        final byte[] expected = ByteBuffer.allocate(Long.BYTES).putLong(100000L).array();
        Assertions.assertEquals(0, Arrays.compare(expected, bytes));
    }

    @Test
    public void testUnsupportedType() {
        Object generic = new Object();
        final Binary binary = new ObjectToBinary(generic);
        HbsRuntimeException ex = Assertions.assertThrows(HbsRuntimeException.class, binary::bytes);
        String expectedMessage = "Error getting binary form";
        Assertions.assertTrue(ex.getMessage().startsWith(expectedMessage));
    }
}
