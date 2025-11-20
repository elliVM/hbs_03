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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class MetaRowKeyTest {

    @Test
    public void testRowKey() {
        final long streamId = 12345L;
        final long logtime = 9876543210L;
        final long logfileId = 54321L;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final byte[] bytes = metaRowKey.bytes();
        Assertions.assertEquals(24, bytes.length, "byte array length should be 24");
    }

    @Test
    public void testRowKeyValues() {
        final long streamId = 12345L;
        final long logtime = 9876543210L;
        final long logfileId = 54321L;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final byte[] rowKeyBytes = metaRowKey.bytes();
        final ByteBuffer buffer = ByteBuffer.wrap(rowKeyBytes).order(ByteOrder.BIG_ENDIAN);
        Assertions.assertEquals(streamId, buffer.getLong());
        Assertions.assertEquals(9876543210L, buffer.getLong());
        Assertions.assertEquals(logfileId, buffer.getLong());
    }

    @Test
    public void testMaxValues() {
        final long streamId = Long.MAX_VALUE;
        final long logtime = Long.MAX_VALUE;
        final long logfileId = Long.MAX_VALUE;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final byte[] bytes = metaRowKey.bytes();
        Assertions.assertEquals(24, bytes.length, "byte array length should be 26");
    }

    @Test
    public void testMinValues() {
        final long streamId = Long.MIN_VALUE;
        final long logtime = Long.MIN_VALUE;
        final long logfileId = Long.MIN_VALUE;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final byte[] bytes = metaRowKey.bytes();
        Assertions.assertEquals(24, bytes.length, "byte array length should be 26");
    }

    @Test
    public void testToString() {
        final long streamId = 12345L;
        final long logtime = 9876543210L;
        final long logfileId = 54321L;
        final MetaRowKey metaRowKey = new MetaRowKey(streamId, logtime, logfileId);
        final String str = metaRowKey.toString();
        // check that values are present as strings
        Assertions.assertTrue(str.contains("<12345>"));
        Assertions.assertTrue(str.contains("<9876543210>"));
        Assertions.assertTrue(str.contains("<54321>"));
        Assertions.assertTrue(str.contains("bytes=<["));
    }

    @Test
    public void testEqualsVerifier() {
        EqualsVerifier.forClass(MetaRowKey.class).verify();
    }

}
