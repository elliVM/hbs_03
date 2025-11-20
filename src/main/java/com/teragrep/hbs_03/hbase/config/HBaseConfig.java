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
package com.teragrep.hbs_03.hbase.config;

import org.apache.hadoop.conf.Configuration;
import com.teragrep.cnf_01.ConfigurationException;
import com.teragrep.hbs_03.HbsRuntimeException;
import com.teragrep.hbs_03.Source;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;

/**
 * HBase configuration HBase from arguments
 */
public final class HBaseConfig implements Source<Configuration> {

    private final com.teragrep.cnf_01.Configuration config;
    private final String prefix;
    private final String filePrefix;

    public HBaseConfig(final com.teragrep.cnf_01.Configuration config) {
        this(config, "hbs.hadoop.");
    }

    public HBaseConfig(final com.teragrep.cnf_01.Configuration config, final String prefix) {
        this(config, prefix, prefix + "config.path");
    }

    public HBaseConfig(final com.teragrep.cnf_01.Configuration config, final String prefix, final String filePrefix) {
        this.config = config;
        this.prefix = prefix;
        this.filePrefix = filePrefix;
    }

    @Override
    public Configuration value() {
        final Map<String, String> map;
        try {
            map = config.asMap();
        }
        catch (final ConfigurationException e) {
            throw new HbsRuntimeException("Error getting configuration", e);
        }
        return sourceFromMap(map).value();
    }

    /** builds a source interface with all options and resources set from a options map */
    private Source<Configuration> sourceFromMap(final Map<String, String> map) {

        Source<Configuration> source = new HBaseConfigWithRequiredOptionsSet(
                new Configuration(HBaseConfiguration.create())
        );

        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();

            if (key.matches(filePrefix)) {
                source = new HBaseConfigWithResource(source, value);

            }
            else {
                source = new HBaseConfigWithOption(source, prefix, key, value);
            }
        }

        return source;
    }
}
