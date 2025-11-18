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
package com.teragrep.hbs_03.hbase;

import com.teragrep.hbs_03.Source;
import org.apache.hadoop.conf.Configuration;

public final class HBaseConfigWithRequiredOptionsSet implements Source<Configuration> {

    private final Configuration config;

    public HBaseConfigWithRequiredOptionsSet(final Configuration config) {
        this.config = config;
    }

    @Override
    public Configuration value() {
        // add default values if not set
        config.setIfUnset("hbase.zookeeper.quorum", "localhost"); // required for connection
        config.setIfUnset("hbase.zookeeper.property.clientProt", "2181"); // default zookeeper port
        config.setIfUnset("hbase.client.retries.number", "10"); // retries for failed request
        config.setIfUnset("hbase.client.pause", "150"); // pause between retries ms
        config.setIfUnset("hbase.client.scanner.timeout.period", "60000"); // scanner timeout ms
        config.setIfUnset("hbase.rpc.timeout", "60000"); // rpc timeout ms
        config.setIfUnset("hbase.client.operation.timeout", "60000"); // operation timeout ms
        config.setIfUnset("hbase.client.write.buffer", "2097152"); // write buffer size bytes
        config.setIfUnset("hbase.regionserver.durability", "SYNC_WAL"); // default safest data durability
        return config;

    }
}
