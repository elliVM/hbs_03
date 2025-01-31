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
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SQLTempTableTest {

    private MariaDBContainer<?> mariadb;
    private Connection conn;

    @BeforeEach
    public void setup() {
        mariadb = Assertions
                .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withDatabaseName("journaldb").withUsername("root").withPassword("rootpassword"));
        mariadb.start();
        conn = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
    }

    @AfterEach
    public void tearDown() {
        assertDoesNotThrow(() -> conn.prepareStatement("DROP TEMPORARY TABLE IF EXISTS `tempTable`").execute());
        mariadb.stop();
    }

    @Test
    public void testCreation() {
        final DSLContext ctx = DSL.using(conn);
        final SQLTempTable tempTable = new SQLTempTable(ctx, "test_table");
        Assertions.assertDoesNotThrow(tempTable::create);
        Assertions.assertDoesNotThrow(tempTable::create);

        final ResultSet result = assertDoesNotThrow(
                () -> conn.prepareStatement("SHOW COLUMNS FROM `test_table`").executeQuery()
        );

        final Map<String, String> expectedMap = new HashMap<>();

        expectedMap.put("id", "bigint(20) unsigned");
        expectedMap.put("epoch", "int(11)");
        expectedMap.put("expiration", "date");
        expectedMap.put("path", "varchar(255)");
        expectedMap.put("name", "varchar(255)");
        expectedMap.put("archived", "timestamp");
        expectedMap.put("checksum", "char(64)");
        expectedMap.put("etag", "varchar(255)");
        expectedMap.put("logtag", "varchar(255)");
        expectedMap.put("size", "bigint(20) unsigned");
        expectedMap.put("meta", "longtext");
        expectedMap.put("source", "varchar(255)");
        expectedMap.put("category", "varchar(255)");
        expectedMap.put("bucket", "varchar(255)");
        expectedMap.put("host", "varchar(255)");
        expectedMap.put("stream_tag", "varchar(255)");
        expectedMap.put("log_group", "varchar(255)");
        expectedMap.put("directory", "varchar(255)");

        Assertions.assertDoesNotThrow(() -> {
            int loops = 0;
            while (result.next()) {
                final String fieldName = result.getString("Field");
                final String fieldType = result.getString("Type");
                Assertions
                        .assertTrue(expectedMap.containsKey(fieldName), "Field <" + fieldName + "> is present in expected field names");
                Assertions
                        .assertEquals(fieldType, expectedMap.get(fieldName), "Field <" + fieldName + "> should have expected type <" + expectedMap.get(fieldName) + ">");
                loops++;
            }
            Assertions.assertEquals(18, loops, "Temp table should have 18 fields");
        });
    }

    @Test
    public void testInsert() {
        final DSLContext ctx = DSL.using(conn);
        final SQLTempTable tempTable = new SQLTempTable(ctx, "test_table");
        Assertions.assertDoesNotThrow(tempTable::create);

        final String sql = "INSERT INTO `test_table` (id, epoch, expiration, path, name, archived, checksum, etag, logtag, size, meta, source, category, bucket, host, stream_tag, log_group, directory)"
                + "VALUES (1, 162680830, '2025-01-23', '/path/to/logfile', 'original_filename.log', '2010-10-10 10:10:10', 'd2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2', 'etagvalue', 'logtagvalue', 1048576, '{\"key\": \"value\"}', 'source_name', 'category_name', 'bucket_name', 'host_name', 'stream_tag', 'log_group', 'stream_dir');";
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement(sql).execute());

        final String select = "SELECT * FROM `test_table`";
        final ResultSet result = assertDoesNotThrow(() -> conn.prepareStatement(select).executeQuery());
        Assertions.assertDoesNotThrow(() -> {

            int loops = 0;
            while (result.next()) {
                Assertions.assertEquals("162680830", result.getString("epoch"), "epoch was wrong");
                Assertions.assertEquals("2025-01-23", result.getString("expiration"), "expiration was wrong");
                Assertions.assertEquals("/path/to/logfile", result.getString("path"), "path was wrong");
                Assertions.assertEquals("original_filename.log", result.getString("name"), "name was wrong");
                Assertions.assertEquals("2010-10-10 10:10:10", result.getString("archived"), "archived was wrong");
                Assertions
                        .assertEquals("d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2", result.getString("checksum"), "checksum was wrong");
                Assertions.assertEquals("etagvalue", result.getString("etag"), "etag was wrong");
                Assertions.assertEquals("logtagvalue", result.getString("logtag"), "logtag was wrong");
                Assertions.assertEquals("1048576", result.getString("size"), "size was wrong");
                Assertions.assertEquals("{\"key\": \"value\"}", result.getString("meta"), "meta was wrong");
                Assertions.assertEquals("source_name", result.getString("source"), "source was wrong");
                Assertions.assertEquals("category_name", result.getString("category"), "category was wrong");
                Assertions.assertEquals("bucket_name", result.getString("bucket"), "bucket was wrong");
                Assertions.assertEquals("host_name", result.getString("host"), "host was wrong");
                Assertions.assertEquals("stream_tag", result.getString("stream_tag"), "stream_tag was wrong");
                Assertions.assertEquals("log_group", result.getString("log_group"), "log_group was wrong");
                Assertions.assertEquals("stream_dir", result.getString("directory"), "directory was wrong");

                loops++;
            }
            Assertions.assertEquals(1, loops, "Result set should only have 1 row");
        });
    }

    @Test
    public void testTruncate() {
        final DSLContext ctx = DSL.using(conn);
        final SQLTempTable tempTable = new SQLTempTable(ctx, "test_table");
        Assertions.assertDoesNotThrow(tempTable::create);

        final String sql = "INSERT INTO `test_table` (id, epoch, expiration, path, name, archived, checksum, etag, logtag, size, meta, source, category, bucket, host, stream_tag, log_group, directory)"
                + "VALUES (1, 162680830, '2025-01-23', '/path/to/logfile', 'original_filename.log', '2010-10-10 10:10:10', 'd2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2', 'etagvalue', 'logtagvalue', 1048576, '{\"key\": \"value\"}', 'source_name', 'category_name', 'bucket_name', 'host_name', 'stream_tag', 'log_group', 'stream_dir');";
        Assertions.assertDoesNotThrow(() -> conn.prepareStatement(sql).execute());

        tempTable.truncate();

        final String select = "SELECT * FROM `test_table`";
        final ResultSet result = assertDoesNotThrow(() -> conn.prepareStatement(select).executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            int loops = 0;
            while (result.next()) {
                loops++;
            }
            Assertions.assertEquals(0, loops, "Result set should not have any rows");
        });
    }
}
