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
import com.teragrep.cnf_01.ConfigurationException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public final class DatabaseClientFactory implements Factory<DatabaseClient> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClientFactory.class);

    private final Configuration config;
    private final String prefix;

    public DatabaseClientFactory(final Configuration config) {
        this(config, "hbs.db.");
    }

    public DatabaseClientFactory(final Configuration config, final String prefix) {
        this.config = config;
        this.prefix = prefix;
    }

    public DatabaseClient object() {
        final String url;
        final String username;
        final String password;
        final String journaldbName;
        final String streamdbName;
        final String bloomdbName;
        final int batchSize;

        try {
            final Map<String, String> map = config.asMap();
            validate(map);
            url = map.get(prefix + "url");
            username = map.get(prefix + "username");
            password = map.get(prefix + "password");
            journaldbName = map.getOrDefault(prefix + "journaldb.name", "journaldb");
            streamdbName = map.getOrDefault(prefix + "streamdb.name", "streamdb");
            bloomdbName = map.getOrDefault(prefix + "bloomdb.name", "bloomdb");
            batchSize = Integer.parseInt(map.getOrDefault(prefix + "batch.size", "5000"));
        }
        catch (final ConfigurationException e) {
            throw new HbsRuntimeException("Error getting configuration", e);
        }

        final Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(streamdbName), new MappedSchema().withInput("journaldb").withOutput(journaldbName), new MappedSchema().withInput("bloomdb").withOutput(bloomdbName)));

        final DatabaseClient client;
        try {
            final Connection conn = DriverManager.getConnection(url, username, password);
            final DSLContext ctx = DSL.using(conn, SQLDialect.MYSQL, settings);
            client = new DatabaseClient(ctx, conn, batchSize);
        }
        catch (final SQLException e) {
            throw new HbsRuntimeException("Error creating database client", e);
        }

        return client;
    }

    private void validate(final Map<String, String> map) {
        if (!map.containsKey(prefix + "url")) {
            throw new IllegalArgumentException("<" + prefix + "url> option missing");
        }
        if (!map.containsKey(prefix + "username")) {
            throw new IllegalArgumentException("<" + prefix + "username> option missing");
        }
        if (!map.containsKey(prefix + "password")) {
            throw new IllegalArgumentException("<" + prefix + "password> option missing");
        }
        if (!map.containsKey(prefix + "streamdb.name")) {
            LOGGER.info("No <" + prefix + "streamdb.name> option. Using default streamdb name <streamdb>");
        }
        if (!map.containsKey(prefix + "journaldb.name")) {
            LOGGER.info("No <" + prefix + "journaldb.name> option. Using default journaldb name <journaldb>");
        }
        if (!map.containsKey(prefix + "bloomdb.name")) {
            LOGGER.info("No <" + prefix + "bloomdb.name> option. Using default bloomdb name <bloomdb>");
        }
    }
}
