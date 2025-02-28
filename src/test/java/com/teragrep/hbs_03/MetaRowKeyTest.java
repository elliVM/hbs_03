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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class MetaRowKeyTest {

    @Test
    public void testRowKey() {
        final long streamId = 12345L;
        final long logtime = 9876543210L;
        final long logfileId = 54321L;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final byte[] bytes = metaRowKey.bytes();
        Assertions.assertEquals(26, bytes.length, "byte array length should be 25");
        Assertions.assertEquals(0x23, bytes[8], "first separator should be #");
        Assertions.assertEquals(0x23, bytes[17], "second separator should be #");
    }

    @Test
    public void testMaxValues() {
        final long streamId = Long.MAX_VALUE;
        final long logtime = Long.MAX_VALUE;
        final long logfileId = Long.MAX_VALUE;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final byte[] bytes = metaRowKey.bytes();
        Assertions.assertEquals(26, bytes.length, "byte array length should be 25");
        Assertions.assertEquals(0x23, bytes[8], "first separator should be #");
        Assertions.assertEquals(0x23, bytes[17], "second separator should be #");
    }

    @Test
    public void testMinValues() {
        final long streamId = Long.MIN_VALUE;
        final long logtime = Long.MIN_VALUE;
        final long logfileId = Long.MIN_VALUE;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final byte[] bytes = metaRowKey.bytes();
        Assertions.assertEquals(26, bytes.length, "byte array length should be 25");
        Assertions.assertEquals(0x23, bytes[8], "first separator should be #");
        Assertions.assertEquals(0x23, bytes[17], "second separator should be #");
    }

    @Test
    public void testToString() {
        final long streamId = 12345L;
        final long logtime = 9876543210L;
        final long logfileId = 54321L;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final String expected = "RowKey(streamId=<12345>, logtime=9876543210, logfileId=54321)\n"
                + " bytes=<[00 00 00 00 00 00 30 39 23 00 00 00 02 4c b0 16 ea 23 00 00 00 00 00 00 d4 31]>";
        Assertions.assertEquals(expected, metaRowKey.toString());
    }

    @Test
    public void testEqualsVerifier() {
        EqualsVerifier.forClass(MetaRowKey.class).verify();
    }

}
