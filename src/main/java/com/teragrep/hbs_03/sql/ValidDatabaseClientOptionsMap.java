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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public final class ValidDatabaseClientOptionsMap implements OptionValue<Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidDatabaseClientOptionsMap.class);

    private final Map<String, String> map;
    private final String prefix;

    public ValidDatabaseClientOptionsMap(final Map<String, String> map) {
        this(map, "hbs.db.");
    }

    public ValidDatabaseClientOptionsMap(final Map<String, String> map, final String prefix) {
        this.map = map;
        this.prefix = prefix;
    }

    @Override
    public Map<String, String> value() {
        validate();
        return Collections.unmodifiableMap(map);
    }

    /** Ensures that required values are present and no empty options */
    private void validate() {

        final String urlKey = prefix + "url";
        if (!map.containsKey(urlKey) || map.get(urlKey).isEmpty()) {
            throw new IllegalArgumentException("<[" + urlKey + "]> option missing or empty");
        }

        final String usernameKey = prefix + "username";
        if (!map.containsKey(usernameKey) || map.get(usernameKey).isEmpty()) {
            throw new IllegalArgumentException("<[" + usernameKey + "]> option missing or empty");
        }

        final String passwordKey = prefix + "password";
        if (!map.containsKey(passwordKey) || map.get(passwordKey).isEmpty()) {
            throw new IllegalArgumentException("<[" + passwordKey + "]> option missing or empty");
        }

        final String streamdbNameKey = prefix + "streamdb.name";
        if (map.containsKey(streamdbNameKey) && map.get(streamdbNameKey).isEmpty()) {
            throw new IllegalArgumentException("<[" + streamdbNameKey + "]> was empty");
        }
        else if (!map.containsKey(streamdbNameKey)) {
            LOGGER.info("No <{}> option. Using default streamdb name <streamdb>", streamdbNameKey);
        }

        final String journaldbNameKey = prefix + "journaldb.name";
        if (map.containsKey(journaldbNameKey) && map.get(journaldbNameKey).isEmpty()) {
            throw new IllegalArgumentException("<[" + journaldbNameKey + "]> was empty");
        }
        else if (!map.containsKey(journaldbNameKey)) {
            LOGGER.info("No <{}> option. Using default journaldb name <journaldb>", journaldbNameKey);
        }

        final String bloomdbNameKey = prefix + "bloomdb.name";
        if (map.containsKey(bloomdbNameKey) && map.get(bloomdbNameKey).isEmpty()) {
            throw new IllegalArgumentException("<[" + bloomdbNameKey + "]> was empty");
        }
        else if (!map.containsKey(bloomdbNameKey)) {
            LOGGER.info("No <{}> option. Using default bloomdb name <bloomdb>", bloomdbNameKey);
        }
    }
}
