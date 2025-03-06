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
import org.jooq.conf.Settings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public final class DatabaseClientFactory implements Factory<DatabaseClient> {

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

        final Map<String, String> map;
        try {
            map = new ValidDatabaseClientOptionsMap(config.asMap(), prefix).value();
        }
        catch (final ConfigurationException e) {
            throw new HbsRuntimeException("Error getting configuration as map", e);
        }

        final String url = map.get(prefix + "url");
        final String username = map.get(prefix + "username");
        final String password = map.get(prefix + "password");
        final int batchSize = Integer.parseInt(map.getOrDefault(prefix + "batch.size", "5000"));
        final Settings databaseSettings = new DatabaseSettingsFromMap(map, prefix).value();

        final Connection conn;
        try {
            conn = DriverManager.getConnection(url, username, password);
        }
        catch (final SQLException e) {
            throw new HbsRuntimeException("Error creating database client", e);
        }

        return new DatabaseClient(conn, databaseSettings, batchSize);
    }
}
