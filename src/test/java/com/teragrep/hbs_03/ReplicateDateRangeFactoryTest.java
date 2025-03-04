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

import com.teragrep.cnf_01.Configuration;
import com.teragrep.cnf_01.PropertiesConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Properties;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(
        named = "runContainerTests",
        matches = "true"
)
public class ReplicateDateRangeFactoryTest {

    @Container
    private MariaDBContainer<?> mariadb;

    @BeforeAll
    public void setup() {
        mariadb = Assertions
                .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withPrivilegedMode(false).withUsername("user").withPassword("password").withDatabaseName("journaldb"));
        mariadb.start();
    }

    @AfterAll
    public void tearDown() {
        mariadb.stop();
    }

    @Test
    public void testCorrectDates() {
        Properties props = new Properties();
        props.setProperty("hbs.start", "2010-02-03");
        props.setProperty("hbs.end", "2020-02-03");
        props.setProperty("hbs.db.url", mariadb.getJdbcUrl());
        props.setProperty("hbs.db.username", mariadb.getUsername());
        props.setProperty("hbs.db.password", mariadb.getPassword());
        Configuration config = new PropertiesConfiguration(props);
        ReplicateDateRange replicateRange = new ReplicateDateRangeFactory(config).object();
        Assertions.assertEquals(Date.valueOf("2010-02-03"), replicateRange.startDate());
        Assertions.assertEquals(Date.valueOf("2020-02-03"), replicateRange.endDate());
    }

    @Test
    public void testDefaultDates() {
        Properties props = new Properties();
        props.setProperty("hbs.db.url", mariadb.getJdbcUrl());
        props.setProperty("hbs.db.username", mariadb.getUsername());
        props.setProperty("hbs.db.password", mariadb.getPassword());
        Configuration config = new PropertiesConfiguration(props);
        ReplicateDateRange replicateRange = new ReplicateDateRangeFactory(config).object();
        Assertions.assertEquals(Date.valueOf(LocalDate.now().minusDays(1)), replicateRange.startDate());
        Assertions.assertEquals(Date.valueOf(LocalDate.now()), replicateRange.endDate());
    }
}
