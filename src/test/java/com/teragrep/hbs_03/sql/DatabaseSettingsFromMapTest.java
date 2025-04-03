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

import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public final class DatabaseSettingsFromMapTest {

    @Test
    public void testDefault() {
        final Map<String, String> optionsMap = new HashMap<>();
        final String prefix = "test.";
        final DatabaseSettingsFromMap databaseSettingsFromMap = new DatabaseSettingsFromMap(optionsMap, prefix);
        final Settings settings = databaseSettingsFromMap.value();
        final RenderMapping renderMapping = settings.getRenderMapping();
        RenderMapping expected = new RenderMapping()
                .withSchemata(new MappedSchema().withInput("streamdb").withOutput("streamdb"), new MappedSchema().withInput("journaldb").withOutput("journaldb"), new MappedSchema().withInput("bloomdb").withOutput("bloomdb"));
        Assertions.assertEquals(expected, renderMapping);
        Assertions.assertFalse(settings.isExecuteLogging());
    }

    @Test
    public void testExecuteLoggingOption() {
        final String prefix = "test.";
        final Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(prefix + "executeLogging", "true");
        final DatabaseSettingsFromMap databaseSettingsFromMap = new DatabaseSettingsFromMap(optionsMap, prefix);
        final Settings settings = databaseSettingsFromMap.value();
        final RenderMapping renderMapping = settings.getRenderMapping();
        RenderMapping expected = new RenderMapping()
                .withSchemata(new MappedSchema().withInput("streamdb").withOutput("streamdb"), new MappedSchema().withInput("journaldb").withOutput("journaldb"), new MappedSchema().withInput("bloomdb").withOutput("bloomdb"));
        Assertions.assertEquals(expected, renderMapping);
        Assertions.assertTrue(settings.isExecuteLogging());
    }

    @Test
    public void testTableNameMappingsOptions() {
        final String prefix = "test.";
        final Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(prefix + "bloomdb.name", "bloom_test");
        optionsMap.put(prefix + "journaldb.name", "journal_test");
        optionsMap.put(prefix + "streamdb.name", "stream_test");
        final DatabaseSettingsFromMap databaseSettingsFromMap = new DatabaseSettingsFromMap(optionsMap, prefix);
        final Settings settings = databaseSettingsFromMap.value();
        final RenderMapping renderMapping = settings.getRenderMapping();
        RenderMapping expected = new RenderMapping()
                .withSchemata(new MappedSchema().withInput("streamdb").withOutput("stream_test"), new MappedSchema().withInput("journaldb").withOutput("journal_test"), new MappedSchema().withInput("bloomdb").withOutput("bloom_test"));
        Assertions.assertEquals(expected, renderMapping);
        Assertions.assertFalse(settings.isExecuteLogging());
    }
}
