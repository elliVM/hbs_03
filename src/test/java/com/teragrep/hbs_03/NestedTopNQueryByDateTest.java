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

import org.jooq.DSLContext;
import org.jooq.JSON;
import org.jooq.Record;
import org.jooq.Record18;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectLimitPercentStep;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

public class NestedTopNQueryByDateTest {

    //    private final String username = System.getProperty("test.db.username");
    //    private final String password = System.getProperty("test.db.password");
    //    private final String url = System.getProperty("test.db.url");

    private final String username = "streamdb";
    private final String password = "streamdb_pass";
    private final String url = "jdbc:mariadb://192.168.49.2:30601/archiver_journal_tyrael";
    private final Settings settings = new Settings()
            .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput("archiver_streamdb_tyrael"), new MappedSchema().withInput("journaldb").withOutput("archiver_journal_tyrael"), new MappedSchema().withInput("bloomdb").withOutput("bloomdb")));

    private final Connection conn = Assertions
            .assertDoesNotThrow(() -> DriverManager.getConnection(url, username, password));

    @Test
    public void testSelectStep() {
        final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL, settings);
        final Date day = Date.valueOf("2020-10-19");
        final NestedTopNQueryByDate nestedTopNQueryByDate = new NestedTopNQueryByDate(ctx, day);
        final SelectLimitPercentStep<Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>> selectWithLimit = nestedTopNQueryByDate
                .selectStep()
                .limit(10);
        final Result<Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>> result = selectWithLimit
                .fetch();
        Assertions.assertEquals(10, result.size());
        final Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String> firstRecord = result
                .get(0);

        Assertions.assertEquals("id", firstRecord.field1().getName());
        Assertions.assertEquals(ULong.class, firstRecord.field1().getType());
        Assertions.assertNotNull(firstRecord.field1().getValue(firstRecord));

        Assertions.assertEquals("epoch", firstRecord.field2().getName());
        Assertions.assertEquals(Integer.class, firstRecord.field2().getType());
        Assertions.assertNotNull(firstRecord.field2().getValue(firstRecord));

        Assertions.assertEquals("expiration", firstRecord.field3().getName());
        Assertions.assertEquals(Date.class, firstRecord.field3().getType());
        Assertions.assertNotNull(firstRecord.field3().getValue(firstRecord));

        Assertions.assertEquals("path", firstRecord.field4().getName());
        Assertions.assertEquals(String.class, firstRecord.field4().getType());
        Assertions.assertNotNull(firstRecord.field4().getValue(firstRecord));

        Assertions.assertEquals("name", firstRecord.field5().getName());
        Assertions.assertEquals(String.class, firstRecord.field5().getType());
        Assertions.assertNotNull(firstRecord.field5().getValue(firstRecord));

        Assertions.assertEquals("archived", firstRecord.field6().getName());
        Assertions.assertEquals(Timestamp.class, firstRecord.field6().getType());
        Assertions.assertNotNull(firstRecord.field6().getValue(firstRecord));

        Assertions.assertEquals("checksum", firstRecord.field7().getName());
        Assertions.assertEquals(String.class, firstRecord.field7().getType());
        Assertions.assertNotNull(firstRecord.field7().getValue(firstRecord));

        Assertions.assertEquals("etag", firstRecord.field8().getName());
        Assertions.assertEquals(String.class, firstRecord.field8().getType());
        Assertions.assertNotNull(firstRecord.field8().getValue(firstRecord));

        Assertions.assertEquals("logtag", firstRecord.field9().getName());
        Assertions.assertEquals(String.class, firstRecord.field9().getType());
        Assertions.assertNotNull(firstRecord.field9().getValue(firstRecord));

        Assertions.assertEquals("size", firstRecord.field10().getName());
        Assertions.assertEquals(ULong.class, firstRecord.field10().getType());
        Assertions.assertNotNull(firstRecord.field10().getValue(firstRecord));

        Assertions.assertEquals("meta", firstRecord.field11().getName());
        Assertions.assertEquals(JSON.class, firstRecord.field11().getType());
        Assertions.assertNotNull(firstRecord.field11().getValue(firstRecord));

        Assertions.assertEquals("source", firstRecord.field12().getName());
        Assertions.assertEquals(String.class, firstRecord.field12().getType());
        Assertions.assertNotNull(firstRecord.field12().getValue(firstRecord));

        Assertions.assertEquals("category", firstRecord.field13().getName());
        Assertions.assertEquals(String.class, firstRecord.field13().getType());
        Assertions.assertNotNull(firstRecord.field13().getValue(firstRecord));

        Assertions.assertEquals("bucket", firstRecord.field14().getName());
        Assertions.assertEquals(String.class, firstRecord.field14().getType());
        Assertions.assertNotNull(firstRecord.field14().getValue(firstRecord));

        Assertions.assertEquals("host", firstRecord.field15().getName());
        Assertions.assertEquals(String.class, firstRecord.field15().getType());
        Assertions.assertNotNull(firstRecord.field15().getValue(firstRecord));

        Assertions.assertEquals("stream_tag", firstRecord.field16().getName());
        Assertions.assertEquals(String.class, firstRecord.field16().getType());
        Assertions.assertNotNull(firstRecord.field16().getValue(firstRecord));

        Assertions.assertEquals("log_group", firstRecord.field17().getName());
        Assertions.assertEquals(String.class, firstRecord.field17().getType());
        Assertions.assertNotNull(firstRecord.field17().getValue(firstRecord));

        Assertions.assertEquals("directory", firstRecord.field18().getName());
        Assertions.assertEquals(String.class, firstRecord.field18().getType());
        Assertions.assertNotNull(firstRecord.field18().getValue(firstRecord));
    }

    @Test
    public void testSelectIntoTable() {
        final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL, settings);
        final SQLTempTable tempTable = new SQLTempTable(ctx);
        tempTable.create();
        final Date day = Date.valueOf("2020-10-19");
        final NestedTopNQueryByDate nestedTopNQueryByDate = new NestedTopNQueryByDate(ctx, day);

        ctx.insertInto(tempTable.table()).select(nestedTopNQueryByDate.selectStep().limit(10)).execute();

        final Result<Record> result = ctx.selectFrom(tempTable.table()).fetch();
        Assertions.assertEquals(10, result.size());

        List<TempTableRow> tableRowsList = result
                .stream()
                .map(
                        record -> new TempTableRow(
                                (Record18<ULong, Integer, Date, String, String, Timestamp, String, String, String, ULong, JSON, String, String, String, String, String, String, String>) record
                        )
                )
                .collect(Collectors.toList());
        tableRowsList.forEach(System.out::println);
    }
}
