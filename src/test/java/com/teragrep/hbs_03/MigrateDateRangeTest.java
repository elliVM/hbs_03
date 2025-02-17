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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MigrateDateRangeTest {

    // HBase
    final Configuration config = HBaseConfiguration.create();

//    private final String tableName = "logfile_test";
//    private TestingHBaseCluster hbase;
//    private Admin admin;
//    private org.apache.hadoop.hbase.client.Connection hbaseConn;
//    private LogfileHBaseTable table;

    // SQL
    final String username = "streamdb";
    final String password = "streamdb_pass";
    final String url = "jdbc:mariadb://192.168.49.2:30601/archiver_journal_tyrael";
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
        new HBaseClient(config, "logfile_test").close();
    }

    @Test
    public void testRange() {
        final HBaseClient client = new HBaseClient(config, "replication_range_test");
        final DatabaseClient sqlClient = new DatabaseClient(ctx, connection,5000);
        final Date start = Date.valueOf("2015-1-1");
        final Date end = Date.valueOf("2021-1-1");
        final MigrateDateRange migrateDateRange = new MigrateDateRange(start, end, sqlClient, client);

        migrateDateRange.start();
    }
}