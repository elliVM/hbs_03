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
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.jooq.types.UShort;
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
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(
        named = "runContainerTests",
        matches = "true"
)
public final class DestinationTableDayQueryTest {

    @Container
    private MariaDBContainer<?> mariadb;
    private Connection connection;
    final Settings settings = new Settings()
            .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput("streamdb"), new MappedSchema().withInput("journaldb").withOutput("journaldb"), new MappedSchema().withInput("bloomdb").withOutput("bloomdb")));

    @BeforeAll
    public void setup() {

        mariadb = Assertions
                .assertDoesNotThrow(() -> new MariaDBContainer<>(DockerImageName.parse("mariadb:10.5")).withReuse(true).withPrivilegedMode(false).withUsername("user").withPassword("password").withDatabaseName("journaldb"));

        mariadb
                .withCopyFileToContainer(MountableFile.forHostPath("./database/journaldb.sql"), "/docker-entrypoint-initdb.d/journaldb.sql");

        mariadb.start();

        connection = Assertions
                .assertDoesNotThrow(
                        () -> DriverManager
                                .getConnection(mariadb.getJdbcUrl(), mariadb.getUsername(), mariadb.getPassword())
                );
    }

    @AfterAll
    public void tearDown() {
        mariadb.stop();
    }

    @Test
    public void testQuery() {
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        insertRow(ctx);
        final Table<Record1<ULong>> dayTable = new LogfileTableDayQuery(ctx, Date.valueOf("2010-01-01")).asTable();
        Result<Record1<ULong>> dayTableResults = ctx.selectFrom(dayTable).fetch();
        Assertions.assertEquals(1, dayTableResults.size());
    }

    private void insertRow(final DSLContext ctx) {
        ctx
                .insertInto(JOURNALDB.BUCKET)
                .set(JOURNALDB.BUCKET.ID, UShort.valueOf(1))
                .set(JOURNALDB.BUCKET.NAME, "bucket")
                .execute();

        ctx
                .insertInto(JOURNALDB.HOST)
                .set(JOURNALDB.HOST.ID, UShort.valueOf(1))
                .set(JOURNALDB.HOST.NAME, "host")
                .execute();

        ctx
                .insertInto(JOURNALDB.CATEGORY)
                .set(JOURNALDB.CATEGORY.ID, UShort.valueOf(1))
                .set(JOURNALDB.CATEGORY.NAME, "category")
                .execute();

        ctx
                .insertInto(JOURNALDB.SOURCE_SYSTEM)
                .set(JOURNALDB.SOURCE_SYSTEM.ID, UShort.valueOf(1))
                .set(JOURNALDB.SOURCE_SYSTEM.NAME, "source")
                .execute();

        ctx
                .insertInto(JOURNALDB.LOGFILE)
                .set(JOURNALDB.LOGFILE.LOGDATE, java.sql.Date.valueOf("2010-01-01"))
                .set(JOURNALDB.LOGFILE.EXPIRATION, java.sql.Date.valueOf("2010-01-01"))
                .set(JOURNALDB.LOGFILE.BUCKET_ID, UShort.valueOf(1))
                .set(JOURNALDB.LOGFILE.PATH, "path/to/logfile")
                .set(JOURNALDB.LOGFILE.HOST_ID, UShort.valueOf(1))
                .set(JOURNALDB.LOGFILE.ORIGINAL_FILENAME, "logfile.log")
                .set(JOURNALDB.LOGFILE.ARCHIVED, java.sql.Timestamp.valueOf("2010-01-01 00:00:00"))
                .set(JOURNALDB.LOGFILE.FILE_SIZE, ULong.valueOf(123456L))
                .set(JOURNALDB.LOGFILE.SHA256_CHECKSUM, "TuWQgQ0BiOKbRNLQ5sxFLCljisHg30TNe0C96gUYRec")
                .set(JOURNALDB.LOGFILE.ARCHIVE_ETAG, "etagvalue")
                .set(JOURNALDB.LOGFILE.LOGTAG, "logtag")
                .set(JOURNALDB.LOGFILE.SOURCE_SYSTEM_ID, UShort.valueOf(1))
                .set(JOURNALDB.LOGFILE.CATEGORY_ID, UShort.valueOf(1))
                .set(JOURNALDB.LOGFILE.UNCOMPRESSED_FILE_SIZE, ULong.valueOf(123456L))
                .execute();

        ctx
                .insertInto(JOURNALDB.LOGFILE)
                .set(JOURNALDB.LOGFILE.LOGDATE, java.sql.Date.valueOf("2011-01-01"))
                .set(JOURNALDB.LOGFILE.EXPIRATION, java.sql.Date.valueOf("2010-01-01"))
                .set(JOURNALDB.LOGFILE.BUCKET_ID, UShort.valueOf(1))
                .set(JOURNALDB.LOGFILE.PATH, "path/to/logfile2")
                .set(JOURNALDB.LOGFILE.HOST_ID, UShort.valueOf(1))
                .set(JOURNALDB.LOGFILE.ORIGINAL_FILENAME, "logfile2.log")
                .set(JOURNALDB.LOGFILE.ARCHIVED, java.sql.Timestamp.valueOf("2011-01-01 00:00:00"))
                .set(JOURNALDB.LOGFILE.FILE_SIZE, ULong.valueOf(123456L))
                .set(JOURNALDB.LOGFILE.SHA256_CHECKSUM, "TuWQgQ0BiOKbRNLQ5sxFLCljisHg40TNe0C96gUYRec")
                .set(JOURNALDB.LOGFILE.ARCHIVE_ETAG, "etagvalue2")
                .set(JOURNALDB.LOGFILE.LOGTAG, "logtag2")
                .set(JOURNALDB.LOGFILE.SOURCE_SYSTEM_ID, UShort.valueOf(1))
                .set(JOURNALDB.LOGFILE.CATEGORY_ID, UShort.valueOf(1))
                .set(JOURNALDB.LOGFILE.UNCOMPRESSED_FILE_SIZE, ULong.valueOf(123456L))
                .execute();
    }
}
