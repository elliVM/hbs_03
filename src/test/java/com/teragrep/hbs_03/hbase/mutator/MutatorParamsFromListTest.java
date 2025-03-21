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
package com.teragrep.hbs_03.hbase.mutator;

import com.teragrep.hbs_03.HbsRuntimeException;
import com.teragrep.hbs_03.Source;
import com.teragrep.hbs_03.hbase.MetaRow;
import com.teragrep.hbs_03.sql.MockS3MetaData;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.jooq.DSLContext;
import org.jooq.Record21;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class MutatorParamsFromListTest {

    // SQL Mock
    final MockS3MetaData provider = new MockS3MetaData();
    final MockConnection connection = new MockConnection(provider);
    final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);

    @Test
    public void testMinimumSize() {
        final Put row = new MetaRow(
                (Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>) ctx
                        .fetch("ONE_ROW")
                        .get(0)
        ).put();
        final List<Put> rowList = Arrays.asList(row);
        final long minimumSize = 2 * 1024 * 1024; // 2MB in byte size
        final BufferedMutatorParams params = new MutatorParamsFromList(
                rowList,
                TableName.valueOf("test"),
                new MutatorConfiguration(true, 2.0)
        ).value();

        final long bufferSize = params.getWriteBufferSize();
        Assert.assertEquals(minimumSize, bufferSize);
    }

    @Test
    public void testDynamicSize() {
        final Put row = new MetaRow(
                (Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>) ctx
                        .fetch("ONE_ROW")
                        .get(0)
        ).put();
        final int rowListSize = 1000;
        final double overheadMultiplier = 3.0;
        final List<Put> rowList = new ArrayList<>(rowListSize);
        for (int i = 0; i < rowListSize; i++) {
            rowList.add(row);
        }
        final long estimatedBufferSize = Math.round(rowListSize * (row.heapSize() * overheadMultiplier));
        final BufferedMutatorParams params = new MutatorParamsFromList(
                rowList,
                TableName.valueOf("test"),
                new MutatorConfiguration(true, 3.0)
        ).value();
        final long bufferSize = params.getWriteBufferSize();
        Assert.assertEquals(estimatedBufferSize, bufferSize);
    }

    @Test
    public void testMaxCalculatedBufferSize() {
        final Put row = new MetaRow(
                (Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>) ctx
                        .fetch("ONE_ROW")
                        .get(0)
        ).put();
        final List<Put> rowList = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            rowList.add(row);
        }
        final double overheadMultiplier = 5.0;
        final BufferedMutatorParams params = new MutatorParamsFromList(
                rowList,
                TableName.valueOf("test"),
                new MutatorConfiguration(true, overheadMultiplier)
        ).value();
        final long bufferSize = params.getWriteBufferSize();
        Assert.assertEquals(67108864, bufferSize); // 64MB
    }

    @Test
    public void testMultiplierTooSmall() {
        final Put row = new MetaRow(
                (Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>) ctx
                        .fetch("ONE_ROW")
                        .get(0)
        ).put();
        final List<Put> rowList = Arrays.asList(row);
        final Source<BufferedMutatorParams> paramsSource = new MutatorParamsFromList(
                rowList,
                TableName.valueOf("test"),
                new MutatorConfiguration(true, 0.9)
        );
        final HbsRuntimeException ex = Assertions.assertThrows(HbsRuntimeException.class, paramsSource::value);
        final String expectedMessage = "Overhead multiplier was not between 1-5 (caused by: IllegalAccessError: Illegal overhead multiplier <0.9>)";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testMultiplierTooLarge() {
        final Put row = new MetaRow(
                (Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>) ctx
                        .fetch("ONE_ROW")
                        .get(0)
        ).put();
        final List<Put> rowList = Arrays.asList(row);
        final Source<BufferedMutatorParams> paramsSource = new MutatorParamsFromList(
                rowList,
                TableName.valueOf("test"),
                new MutatorConfiguration(true, 5.1)
        );
        final HbsRuntimeException ex = Assertions.assertThrows(HbsRuntimeException.class, paramsSource::value);
        final String expectedMessage = "Overhead multiplier was not between 1-5 (caused by: IllegalAccessError: Illegal overhead multiplier <5.1>)";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testDefaultBuffer() {
        final Put row = new MetaRow(
                (Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>) ctx
                        .fetch("ONE_ROW")
                        .get(0)
        ).put();
        final List<Put> rowList = Arrays.asList(row);
        final long minimumSize = 2 * 1024 * 1024; // 2MB in byte size, min size for default buffer
        final BufferedMutatorParams params = new MutatorParamsFromList(
                rowList,
                TableName.valueOf("test"),
                new MutatorConfiguration(false)
        ).value();
        final long bufferSize = params.getWriteBufferSize();
        Assert.assertEquals(minimumSize, bufferSize);
    }

}
