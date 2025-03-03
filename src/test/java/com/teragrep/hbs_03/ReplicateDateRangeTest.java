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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled("Requires local MariaDB and HBase")
public final class ReplicateDateRangeTest {

    final Configuration config = HBaseConfiguration.create();
    final String testTableName = "replication_range_test";
    final String username = "streamdb";
    final String password = "streamdb_pass";
    final String url = "jdbc:mariadb://192.168.49.2:30601/archiver_journal_tyrael";
    final Settings settings = new Settings()
            .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput("archiver_streamdb_tyrael"), new MappedSchema().withInput("journaldb").withOutput("archiver_journal_tyrael"), new MappedSchema().withInput("bloomdb").withOutput("bloomdb")));
    final Connection connection = Assertions
            .assertDoesNotThrow(() -> DriverManager.getConnection(url, username, password));
    final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
    HBaseClient client;

    @BeforeAll
    public void setup() {
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        this.client = new HBaseClient(config, testTableName, true);
        client.destinationTable().delete();
        client.destinationTable().create();
    }

    @AfterAll
    public void tearDown() {
        client.close();
    }

    @Test
    public void testHadoopConfig() {
        System.out.println(config);
    }

    @Test
    public void testRange() {
        final DatabaseClient sqlClient = new DatabaseClient(ctx, connection, 5000);
        final Date start = Date.valueOf("2015-1-1");
        final Date end = Date.valueOf("2025-1-1");
        final ReplicateDateRange replicateDateRange = new ReplicateDateRange(start, end, sqlClient, client);

        replicateDateRange.start();
    }
}
