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

import com.teragrep.hbs_03.hbase.Row;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
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
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(
        named = "skipContainerTests",
        matches = "true"
)
public final class LogfileTableFlatQueryTest {

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
        final Record3<ULong, ULong, Integer> result = ctx
                .select(DSL.min(JOURNALDB.LOGFILE.ID), DSL.max(JOURNALDB.LOGFILE.ID), DSL.count())
                .from(JOURNALDB.LOGFILE)
                .fetchOne();
        Assertions.assertNotNull(result);
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
    public void testFlatQuery() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        new HostMappingTempTable(ctx).createIfNotExists();
        final LogfileTableIdRangeQuery rangeQuery = new LogfileTableIdRangeQuery(ctx, 101, 200);
        final LogfileTableFlatQuery logfileTableFlatQuery = new LogfileTableFlatQuery(ctx, rangeQuery);
        final List<Row> results = logfileTableFlatQuery.resultRowList();
        Assertions.assertEquals(100, results.size());
        int loops = 0;
        for (final Row row : results) {
            Assertions.assertDoesNotThrow(row::put);
            Assertions.assertDoesNotThrow(row::id);
            Assertions.assertDoesNotThrow(row::rowKey);
            loops++;
        }

        Assertions.assertEquals(100, loops);
    }

    @Test
    public void testFlatQueryJoinsIncrementally() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        new HostMappingTempTable(ctx).createIfNotExists();

        // SQL queries with joins added progressively
        final String[] queries = new String[] {
                "select count(*) from host_mapping_temp_table",
                "select count(*) from logfile where id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id join bucket on logfile.bucket_id = bucket.id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id join bucket on logfile.bucket_id = bucket.id join metadata_value on logfile.id = metadata_value.logfile_id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id join bucket on logfile.bucket_id = bucket.id join metadata_value on logfile.id = metadata_value.logfile_id join logtag on logfile.logtag_id = logtag.id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id join bucket on logfile.bucket_id = bucket.id join metadata_value on logfile.id = metadata_value.logfile_id join logtag on logfile.logtag_id = logtag.id join source_system on logfile.source_system_id = source_system.id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id join bucket on logfile.bucket_id = bucket.id join metadata_value on logfile.id = metadata_value.logfile_id join logtag on logfile.logtag_id = logtag.id join source_system on logfile.source_system_id = source_system.id join category on logfile.category_id = category.id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id join bucket on logfile.bucket_id = bucket.id join metadata_value on logfile.id = metadata_value.logfile_id join logtag on logfile.logtag_id = logtag.id join source_system on logfile.source_system_id = source_system.id join category on logfile.category_id = category.id join ci on logfile.ci_id = ci.id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id join bucket on logfile.bucket_id = bucket.id join metadata_value on logfile.id = metadata_value.logfile_id join logtag on logfile.logtag_id = logtag.id join source_system on logfile.source_system_id = source_system.id join category on logfile.category_id = category.id join ci on logfile.ci_id = ci.id join log_group on host_mapping_temp_table.gid = log_group.id where logfile.id between 100 and 200",
                "select count(*) from logfile join journal_host on logfile.host_id = journal_host.id join host_mapping_temp_table on journal_host.id = host_mapping_temp_table.host_id join bucket on logfile.bucket_id = bucket.id join metadata_value on logfile.id = metadata_value.logfile_id join logtag on logfile.logtag_id = logtag.id join source_system on logfile.source_system_id = source_system.id join category on logfile.category_id = category.id join ci on logfile.ci_id = ci.id join log_group on host_mapping_temp_table.gid = log_group.id join stream on log_group.id = stream.gid and logtag.logtag = stream.tag where logfile.id between 100 and 200"
        };

        final int[] expectedRows = new int[] {
                10, // host_mapping_temp_table
                101, // logfile id 100-200
                101, // join journal_host
                101, // join host_mapping_temp_table
                101, // join bucket
                101, // join metadata_value
                101, // join logtag
                101, // join source_system
                101, // join category
                101, // join ci
                101, // join log_group
                101 // join stream
        };

        for (int i = 0; i < queries.length; i++) {
            final Result<Record> result = ctx.fetch(queries[i]);
            final int count = result.get(0).getValue(0, Integer.class);
            Assertions
                    .assertEquals(
                            expectedRows[i], count, "Query " + (i + 1) + " returned unexpected row count: " + count
                    );
        }
    }
}
