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

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.MappedTable;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(
        named = "runContainerTests",
        matches = "true"
)
public final class LogfileTableIdRangeQueryTest {

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
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        Record3<ULong, ULong, Integer> result = ctx.select(DSL.min(JOURNALDB.LOGFILE.ID), DSL.max(JOURNALDB.LOGFILE.ID), DSL.count())
                .from(JOURNALDB.LOGFILE)
                .fetchOne();
        Assertions.assertEquals(ULong.valueOf(1), result.value1());
        Assertions.assertEquals(ULong.valueOf(10000), result.value2());
        Assertions.assertEquals(10000, result.value3().intValue());
    }

    @AfterAll
    public void tearDown() {
        Assertions.assertDoesNotThrow(connection::close);
        Assertions.assertDoesNotThrow(mariadb::stop);
    }

    @Test
    public void testRangeFullResults() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final LogfileTableIdRangeQuery logfileTableIdRangeQuery = new LogfileTableIdRangeQuery(ctx, 1000L, 3000L);
        final Table<Record1<ULong>> idRangeTable = logfileTableIdRangeQuery.asTable();
        final SelectJoinStep<Record> selectFromTable = ctx.select().from(idRangeTable);
        final Result<Record> results = selectFromTable.fetch();
        Assertions.assertEquals(2001, results.size());
    }

    @Test
    public void testRangePartialResults() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final LogfileTableIdRangeQuery logfileTableIdRangeQuery = new LogfileTableIdRangeQuery(ctx, 9000, 19000);
        final Table<Record1<ULong>> idRangeTable = logfileTableIdRangeQuery.asTable();
        final SelectJoinStep<Record> selectFromTable = ctx.select().from(idRangeTable);
        final Result<Record> results = selectFromTable.fetch();
        Assertions.assertEquals(1001, results.size());
    }

    @Test
    public void testNoResults() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final LogfileTableIdRangeQuery logfileTableIdRangeQuery = new LogfileTableIdRangeQuery(ctx, 100000, 110000);
        final Table<Record1<ULong>> idRangeTable = logfileTableIdRangeQuery.asTable();
        final SelectJoinStep<Record> selectFromTable = ctx.select().from(idRangeTable);
        final Result<Record> results = selectFromTable.fetch();
        Assertions.assertEquals(0, results.size());
    }

    @Test
    public void testCorrectIDs() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        final LogfileTableIdRangeQuery logfileTableIdRangeQuery = new LogfileTableIdRangeQuery(ctx, 100, 200);
        final Table<Record1<ULong>> idRangeTable = logfileTableIdRangeQuery.asTable();
        final SelectJoinStep<Record> selectFromTable = ctx.select().from(idRangeTable);
        final Result<Record> results = selectFromTable.fetch();
        Assertions.assertEquals(101, results.size());
        int loops = 0;
        int currentId = 100;
        for (final Record record : results) {
            final ULong id = record.get("id", ULong.class);
            Assertions.assertEquals(currentId, id.longValue());
            currentId++;
            loops++;
        }
        Assertions.assertEquals(101, loops);
    }
}
