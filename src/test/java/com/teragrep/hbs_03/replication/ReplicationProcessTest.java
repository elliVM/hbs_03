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
package com.teragrep.hbs_03.replication;

import com.teragrep.hbs_03.hbase.HBaseClientImpl;
import com.teragrep.hbs_03.sql.DatabaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.jooq.DSLContext;
import org.jooq.Record3;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(
        named = "skipContainerTests",
        matches = "true"
)
public final class ReplicationProcessTest {

    private TestingHBaseCluster hbase;
    private final MariaDBContainer<?> mariadb = Assertions
            .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withPrivilegedMode(false).withUsername("user").withPassword("password").withDatabaseName("journaldb").withInitScript("setup_database.sql"));
    private final Settings settings = new Settings()
            .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput("journaldb"), new MappedSchema().withInput("journaldb").withOutput("journaldb").withTables(new MappedTable().withInput("host").withOutput("journal_host")), new MappedSchema().withInput("bloomdb").withOutput("journaldb")));
    private Connection sqlConnection;
    private org.apache.hadoop.hbase.client.Connection hbaseConnection;
    private String lastIdFilePath;

    @BeforeAll
    public void setup() {
        // hbase
        final TestingHBaseClusterOption options = TestingHBaseClusterOption
                .builder()
                .numMasters(1)
                .numRegionServers(1)
                .build();
        hbase = TestingHBaseCluster.create(options);
        hbase.getConf().set("hbase.master.hostname", "localhost");
        hbase.getConf().set("hbase.regionserver.hostname", "localhost");
        hbase.getConf().set("hbase.zookeeper.quorum", "localhost");
        hbase.getConf().set("hbase.zookeeper.property.clientPort", "2181");
        Assertions.assertDoesNotThrow(hbase::start);
        hbaseConnection = Assertions.assertDoesNotThrow(() -> ConnectionFactory.createConnection(hbase.getConf()));

        // sql
        Assertions.assertDoesNotThrow(mariadb::start);
        sqlConnection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );

        final DSLContext ctx = DSL.using(sqlConnection, SQLDialect.MYSQL, settings);
        final Record3<ULong, ULong, Integer> result = ctx
                .select(DSL.min(JOURNALDB.LOGFILE.ID), DSL.max(JOURNALDB.LOGFILE.ID), DSL.count())
                .from(JOURNALDB.LOGFILE)
                .fetchOne();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(ULong.valueOf(1), result.value1());
        Assertions.assertEquals(ULong.valueOf(10000), result.value2());
        Assertions.assertEquals(10000, result.value3().intValue());
    }

    @BeforeEach
    public void setupLastIDTempFile() {
        final Path path = Paths.get("src", "test", "resources", "zero_last_id.txt");
        lastIdFilePath = path.toString();
        final LastIdSavedToFile lastIdSavedToFile = new LastIdSavedToFile(0, lastIdFilePath);
        Assertions.assertDoesNotThrow(lastIdSavedToFile::save);
    }

    @AfterAll
    public void tearDown() {
        Assertions.assertDoesNotThrow(sqlConnection::close);
        Assertions.assertDoesNotThrow(hbaseConnection::close);
        Assertions.assertDoesNotThrow(mariadb::stop);
        Assertions.assertDoesNotThrow(hbase::stop);
    }

    @Test
    public void testReplication() {
        final DatabaseClient databaseClient = new DatabaseClient(sqlConnection, settings);
        final HBaseClientImpl hBaseClient = new HBaseClientImpl(hbase.getConf(), "test_logfile");
        final long firstAvailableId = databaseClient.firstAvailableId().longValue();
        Assertions.assertEquals(1L, firstAvailableId);
        final long lastAvailableId = databaseClient.lastId().longValue();
        Assertions.assertEquals(10000L, lastAvailableId);
        final BlockRangeStream blockRangeStream = new BlockRangeStream(firstAvailableId, lastAvailableId, 2000);
        try (
                final ReplicationProcess replicationProcess = new ReplicationProcess(
                        databaseClient,
                        hBaseClient,
                        blockRangeStream,
                        lastIdFilePath
                )
        ) {
            replicationProcess.replicate();
        }
        final TableName tableName = TableName.valueOf("test_logfile");
        Assertions.assertDoesNotThrow(() -> {
            try (final Admin admin = hbaseConnection.getAdmin()) {
                Assertions.assertTrue(admin.tableExists(tableName));
            }
            try (final Table table = hbaseConnection.getTable(tableName)) {
                final ResultScanner scanner = table.getScanner(new Scan());

                int rowCount = 0;
                for (final Result result : scanner) {
                    rowCount++;

                }
                Assertions.assertEquals(10000, rowCount);
            }
        });
    }
}
