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
package com.teragrep.hbs_03.sql;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.MappedTable;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(
        named = "skipContainerTests",
        matches = "true"
)
public final class HostMappingTempTableTest {

    final MariaDBContainer<?> mariadb = Assertions
            .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withPrivilegedMode(false).withUsername("user").withPassword("password").withDatabaseName("journaldb").withInitScript("setup_database.sql"));

    final Settings settings = new Settings()
            .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput("journaldb"), new MappedSchema().withInput("journaldb").withOutput("journaldb").withTables(new MappedTable().withInput("host").withOutput("journal_host")), new MappedSchema().withInput("bloomdb").withOutput("journaldb")));

    Connection connection;

    @BeforeAll
    public void setup() {
        Assertions.assertDoesNotThrow(mariadb::start);
        connection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
    }

    @AfterAll
    public void tearDown() {
        Assertions.assertDoesNotThrow(connection::close);
        Assertions.assertDoesNotThrow(mariadb::stop);
    }

    @Test
    public void testCreation() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final HostMappingTempTable tempTable = new HostMappingTempTable(ctx);
        Assertions.assertDoesNotThrow(tempTable::createIfNotExists);
        final Result<Record> fetchOne = ctx.fetch("SELECT 1 FROM host_mapping_temp_table LIMIT 1");
        Assertions.assertEquals(1, fetchOne.size());
    }

    @Test
    public void testCorrectSize() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final HostMappingTempTable tempTable = new HostMappingTempTable(ctx);
        Assertions.assertDoesNotThrow(tempTable::createIfNotExists);
        final Result<Record> result = ctx.fetch("SELECT host_id, gid FROM host_mapping_temp_table");
        Assertions.assertFalse(result.isEmpty());
        Assertions.assertEquals(10, result.size());
    }

    @Test
    public void testRows() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final HostMappingTempTable tempTable = new HostMappingTempTable(ctx);
        Assertions.assertDoesNotThrow(tempTable::createIfNotExists);
        final Result<Record> result = ctx.fetch("SELECT host_id, gid FROM host_mapping_temp_table LIMIT 1");
        Assertions.assertFalse(result.isEmpty());
        Assertions.assertEquals(1, result.size());
        Record firstRow = result.get(0);
        Assertions.assertNotNull(firstRow.get("host_id"));
        Assertions.assertNotNull(firstRow.get("gid"));
    }

    @Test
    public void testMappingUniqueness() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final HostMappingTempTable tempTable = new HostMappingTempTable(ctx);
        Assertions.assertDoesNotThrow(tempTable::createIfNotExists);
        final Result<Record> withDuplicatesResult = ctx
                .fetch(
                        "SELECT host_id, COUNT(*) AS count FROM host_mapping_temp_table GROUP BY host_id HAVING count > 1"
                );
        Assertions.assertTrue(withDuplicatesResult.isEmpty());
    }

    @Test
    public void testIndexesCreated() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final HostMappingTempTable tempTable = new HostMappingTempTable(ctx);
        Assertions.assertDoesNotThrow(tempTable::createIfNotExists);
        final Result<Record> selectIndexes = ctx.fetch("SHOW INDEX FROM host_mapping_temp_table");
        boolean hasHostIdIndex = selectIndexes
                .stream()
                .anyMatch(record -> "ix_host_mapping_temp_table_host_id".equals(record.get("Key_name")));
        boolean hasHostIdGidCompositeIndex = selectIndexes
                .stream()
                .anyMatch(record -> "cix_host_mapping_temp_table_host_id_gid".equals(record.get("Key_name")));
        boolean allIndexesUnique = selectIndexes
                .stream()
                .filter(record -> "ix_host_mapping_temp_table_host_id".equals(record.get("Key_name")) || "cix_host_mapping_temp_table_host_id_gid".equals(record.get("Key_name"))).allMatch(record -> record.get("Non_unique", Integer.class) == 0);
        Assertions.assertTrue(hasHostIdIndex, "should have index on host_id");
        Assertions.assertTrue(hasHostIdGidCompositeIndex, "should have composite index on host_id and gid");
        Assertions.assertTrue(allIndexesUnique, "should have only unique indexes");

    }

    @Test
    public void testHostIdIndexCorrectColumn() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final HostMappingTempTable tempTable = new HostMappingTempTable(ctx);
        Assertions.assertDoesNotThrow(tempTable::createIfNotExists);
        final Result<Record> indexes = ctx.fetch("SHOW INDEXES FROM host_mapping_temp_table;");
        Assertions.assertFalse(indexes.isEmpty(), "Expected indexes to exist");
        boolean existsForColumn = indexes
                .stream()
                .anyMatch(record -> "ix_host_mapping_temp_table_host_id".equals(record.get("Key_name", String.class)) && "host_id".equals(record.get("Column_name", String.class)));

        Assertions.assertTrue(existsForColumn);
    }

}
