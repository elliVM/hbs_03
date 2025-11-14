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

import com.teragrep.cnf_01.PropertiesConfiguration;
import com.teragrep.hbs_03.hbase.HBaseClientImpl;
import com.teragrep.hbs_03.sql.DatabaseClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Can be used to test replication on a local machine, requires configuration for JDBC and HBase connection. Expects
 * /var/lib/hbs_03/last_processed_id.txt to be present, sets to 0 after all
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfSystemProperty(
        named = "skipContainerTests",
        matches = "true"
)
@Disabled("Manual configurable test")
public final class ManualReplicationProcessTest {

    final Configuration config = HBaseConfiguration.create();
    final String testTableName = "replication_range_test";
    final String username = "streamdb";
    final String password = "streamdb_pass";
    final String url = "jdbc:mariadb://192.168.49.2:30341/archiver_journal_tyrael";
    final Settings settings = new Settings()
            .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput("archiver_streamdb_tyrael"), new MappedSchema().withInput("journaldb").withOutput("archiver_journal_tyrael"), new MappedSchema().withInput("bloomdb").withOutput("bloomdb")));
    final Connection connection = Assertions
            .assertDoesNotThrow(() -> DriverManager.getConnection(url, username, password));
    final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);

    @BeforeAll
    public void setup() {
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
    }

    @AfterAll
    public void tearDown() {
        Assertions.assertDoesNotThrow(() -> new HBaseClientImpl(config, testTableName).close());
    }

    @BeforeEach
    public void resetFile() {
        Assertions.assertDoesNotThrow(() -> new LastIdSavedToFile(0).save());
    }

    @Test
    public void testRange() {
        final HBaseClientImpl client = new HBaseClientImpl(config, "replication_range_test");
        final DatabaseClient sqlClient = new DatabaseClient(ctx, connection);
        final BlockRangeStream blockRangeStream = new BlockRangeStream(
                sqlClient.firstAvailableId(),
                sqlClient.lastId(),
                10000
        );
        try (
                final ReplicationProcess replicateDateRange = new ReplicationProcess(sqlClient, client, blockRangeStream)
        ) {
            replicateDateRange.replicate();
        }
    }

    @Test
    public void testRangeWithFactory() {
        final Properties properties = new Properties();
        properties.put("hbs.db.url", url);
        properties.put("hbs.db.username", username);
        properties.put("hbs.db.password", password);
        properties.put("hbs.db.batchSize", "10000");
        properties.put("hbs.db.journaldb.name", "archiver_journal_tyrael");
        properties.put("hbs.db.streamdb.name", "archiver_streamdb_tyrael");
        properties.put("hbs.db.executeLogging", "true");
        properties.put("hbs.hadoop.logfile.table.name", testTableName);
        properties.put("hbs.hadoop.hbase.zookeeper.quorum", "localhost");
        properties.put("hbs.hadoop.hbase.zookeeper.property.clientPort", "2181");

        final PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration(properties);
        final ReplicationProcessFactory factory = new ReplicationProcessFactory(propertiesConfiguration);
        try (final ReplicationProcess replicateDateRange = factory.object()) {
            replicateDateRange.replicate();
        }
    }
}
