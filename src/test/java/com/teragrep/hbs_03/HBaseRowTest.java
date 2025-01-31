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
import org.jooq.DSLContext;
import org.jooq.JSON;
import org.jooq.Record;
import org.jooq.Record18;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.types.ULong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;

public class HBaseRowTest {

    // SQL Mock
    final MockSQLData provider = new MockSQLData();
    final MockConnection connection = new MockConnection(provider);
    final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL);

    @Test
    public void testRowKey() {
        final Result<Record> result = ctx.fetch("ONE_ROW");
        final HBaseRow row = new HBaseRow(
                (Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>) result
                        .get(0)
        );
        final String expected = String.format("%08x", "directory".hashCode()) + "#-1685179496";
        final String rowKey = row.id();
        Assertions.assertEquals(expected, rowKey);
    }

    @Test
    public void testPut() {
        final Result<Record> result = ctx.fetch("ONE_ROW");
        final HBaseRow row = new HBaseRow(
                (Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>) result
                        .get(0)
        );
        final Put put = row.put();
        String expected = "{\"totalColumns\":17,\"row\":\"c6a01e6d#-1685179496\",\"families\":{\"meta\":[{\"qualifier\":\"pth\",\"vlen\":4,\"tag\":[],\"timestamp\":\"9223372036854775807\"},{\"qualifier\":\"src\",\"vlen\":6,\"tag\":[],\"timestamp\":\"9223372036854775807\"},{\"qualifier\":\"sz\",\"vlen\":8,\"tag\":[],\"timestamp\":\"9223372036854775807\"},{\"qualifier\":\"epoch\",\"vlen\":4,\"tag\":[],\"timestamp\":\"9223372036854775807\"}]},\"ts\":\"9223372036854775807\"}";
        Assertions.assertEquals(expected, put.toString());
    }
}
