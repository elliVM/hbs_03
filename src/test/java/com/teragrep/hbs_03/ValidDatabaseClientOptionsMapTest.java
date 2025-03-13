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
import com.teragrep.hbs_03.sql.ValidDatabaseClientOptionsMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ValidDatabaseClientOptionsMapTest {

    final String prefix = "prefix.";

    @Test
    public void testValid() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "url", "url");
        props.setProperty(prefix + "username", "username");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> map = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(map, prefix);

        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put(prefix + "url", "url");
        expectedMap.put(prefix + "password", "password");
        expectedMap.put(prefix + "username", "username");

        final Map<String, String> value = Assertions.assertDoesNotThrow(valid::value);
        Assertions.assertEquals(expectedMap, value);
    }

    @Test
    public void testURLMissing() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "username", "username");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> map = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(map, prefix);
        final IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, valid::value);
        final String expectedMessage = "<[prefix.url]> option missing or empty";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testUsernameMissing() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "url", "url");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> map = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(map, prefix);
        final IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, valid::value);
        final String expectedMessage = "<[prefix.username]> option missing or empty";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testPasswordMissing() {
        final Properties props = new Properties();
        props.setProperty(prefix + "username", "username");
        props.setProperty(prefix + "url", "url");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> map = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(map, prefix);
        final IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, valid::value);
        final String expectedMessage = "<[prefix.password]> option missing or empty";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testJournalDBName() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "url", "url");
        props.setProperty(prefix + "username", "username");
        props.setProperty(prefix + "journaldb.name", "test_journaldb");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> configMap = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(configMap, prefix);
        Assertions.assertDoesNotThrow(valid::value);
        final Map<String, String> validMap = valid.value();
        final String journalDBname = validMap.get(prefix + "journaldb.name");
        Assertions.assertEquals("test_journaldb", journalDBname);
    }

    @Test
    public void testStreamDBName() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "url", "url");
        props.setProperty(prefix + "username", "username");
        props.setProperty(prefix + "streamdb.name", "test_streamdb");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> configMap = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(configMap, prefix);
        Assertions.assertDoesNotThrow(valid::value);
        final Map<String, String> validMap = valid.value();
        final String streamDBName = validMap.get(prefix + "streamdb.name");
        Assertions.assertEquals("test_streamdb", streamDBName);
    }

    @Test
    public void testBloomDBName() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "url", "url");
        props.setProperty(prefix + "username", "username");
        props.setProperty(prefix + "bloomdb.name", "test_bloomdb");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> configMap = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(configMap, prefix);
        Assertions.assertDoesNotThrow(valid::value);
        final Map<String, String> validMap = valid.value();
        final String bloomDBName = validMap.get(prefix + "bloomdb.name");
        Assertions.assertEquals("test_bloomdb", bloomDBName);
    }

    @Test
    public void testJournalDBNameEmpty() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "url", "url");
        props.setProperty(prefix + "username", "username");
        props.setProperty(prefix + "journaldb.name", "");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> map = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(map, prefix);
        final IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, valid::value);
        final String expectedMessage = "<[prefix.journaldb.name]> was empty";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testStreamDBNameEmpty() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "url", "url");
        props.setProperty(prefix + "username", "username");
        props.setProperty(prefix + "streamdb.name", "");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> map = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(map, prefix);
        final IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, valid::value);
        final String expectedMessage = "<[prefix.streamdb.name]> was empty";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testBloomDBNameEmpty() {
        final Properties props = new Properties();
        props.setProperty(prefix + "password", "password");
        props.setProperty(prefix + "url", "url");
        props.setProperty(prefix + "username", "username");
        props.setProperty(prefix + "bloomdb.name", "");
        final Configuration propertiesConfiguration = new PropertiesConfiguration(props);
        final Map<String, String> map = Assertions.assertDoesNotThrow(propertiesConfiguration::asMap);
        final ValidDatabaseClientOptionsMap valid = new ValidDatabaseClientOptionsMap(map, prefix);
        final IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, valid::value);
        final String expectedMessage = "<[prefix.bloomdb.name]> was empty";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }
}
