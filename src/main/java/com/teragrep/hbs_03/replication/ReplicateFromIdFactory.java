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
package com.teragrep.hbs_03.replication;

import com.teragrep.cnf_01.Configuration;
import com.teragrep.cnf_01.ConfigurationException;
import com.teragrep.hbs_03.Factory;
import com.teragrep.hbs_03.HbsRuntimeException;
import com.teragrep.hbs_03.hbase.HBaseClient;
import com.teragrep.hbs_03.hbase.HBaseClientFactory;
import com.teragrep.hbs_03.sql.DatabaseClient;
import com.teragrep.hbs_03.sql.DatabaseClientFactory;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class ReplicateFromIdFactory implements Factory<ReplicateFromId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicateFromIdFactory.class);

    private final Configuration config;
    private final String prefix;

    public ReplicateFromIdFactory(final Configuration config) {
        this(config, "hbs.");
    }

    public ReplicateFromIdFactory(final Configuration config, final String prefix) {
        this.config = config;
        this.prefix = prefix;
    }

    @Override
    public ReplicateFromId object() {

        final DatabaseClient databaseClient = new DatabaseClientFactory(config, prefix + "db.").object();
        final HBaseClient hbaseClient = new HBaseClientFactory(config, prefix + "hadoop.").object();

        final long lastIdFromFile = new LastIdReadFromFile().read();
        final long minIdInDatabase = databaseClient.firstAvailableId().longValue();

        final long startId;
        if (minIdInDatabase > lastIdFromFile) {
            LOGGER.info("First available id value was larger than given start id");
            startId = minIdInDatabase;
        }
        else {
            startId = lastIdFromFile;
        }

        final long maxBatch;
        try {
            final Map<String, String> map = config.asMap();
            final String maxBatchSizeKey = prefix + "db.batchSize";
            final String maxBatchSizeOption = map.getOrDefault(maxBatchSizeKey, "10000");
            maxBatch = Long.parseLong(maxBatchSizeOption);
            LOGGER.debug("Set batch size <{}>", maxBatch);
        }
        catch (final ConfigurationException | NumberFormatException e) {
            throw new HbsRuntimeException("Error getting max batch size option", e);
        }

        final ULong endId = databaseClient.lastId();
        final LogfileIdStream logfileIdStream = new LogfileIdStream(startId, endId.longValue(), maxBatch);

        return new ReplicateFromId(databaseClient, hbaseClient, logfileIdStream);
    }
}
