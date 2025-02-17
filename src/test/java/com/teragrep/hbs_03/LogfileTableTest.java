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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LogfileTableTest {
    HBaseTestingUtility hbase = new HBaseTestingUtility();
    Connection conn;
    Admin admin;

    @BeforeAll
    public void setup() {
        Assertions.assertDoesNotThrow(() -> {
            hbase.getConfiguration().set("hadoop.tmp.dir", "/tmp/hbase-test");
            hbase.getConfiguration().set("hbase.zookeeper.quorum", "localhost");
            hbase.getConfiguration().set("hbase.zookeeper.property.clientPort", "2181");
            hbase.startMiniCluster();
            conn = hbase.getConnection();
            admin = conn.getAdmin();
        });
    }

    @BeforeEach
    public void beforeEach() {
        new LogfileTable(conn, "test_table").delete();
    }

    @AfterAll
    public void tearDown() {
        Assertions.assertDoesNotThrow(() -> {
            admin.close();
        });
        Assertions.assertDoesNotThrow(() -> {
            conn.close();
        });
        Assertions.assertDoesNotThrow(() -> {
            hbase.shutdownMiniCluster();
        });
    }

    @Test
    public void testCreate() {
        LogfileTable table = new LogfileTable(conn, "test_table");
        Assertions.assertDoesNotThrow(() -> {
            Assertions.assertFalse(admin.tableExists(TableName.valueOf("test_table")));
        });
        Assertions.assertDoesNotThrow(table::create);
        // checks that create is not run if the table already exists
        Assertions.assertDoesNotThrow(table::create);
        Assertions.assertDoesNotThrow(() -> {
            final boolean exists = admin.tableExists(TableName.valueOf("test_table"));
            Assertions.assertTrue(exists);
        });
    }
}
