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

import com.teragrep.hbs_03.hbase.HBaseClient;
import com.teragrep.hbs_03.hbase.HBaseTable;
import com.teragrep.hbs_03.sql.DatabaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replicate SQL rows to HBase between the range for the LogfileIdStream
 */
public final class ReplicateFromId implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicateFromId.class);

    private final DatabaseClient databaseClient;
    private final HBaseClient hbaseClient;
    private final LogfileIdStream logfileIdStream;

    public ReplicateFromId(
            final DatabaseClient databaseClient,
            final HBaseClient hbaseClient,
            final LogfileIdStream logfileIdStream
    ) {
        this.databaseClient = databaseClient;
        this.hbaseClient = hbaseClient;
        this.logfileIdStream = logfileIdStream;
    }

    public void replicate() {
        final long startTime = System.nanoTime(); // logging

        final HBaseTable destinationTable = hbaseClient.destinationTable();
        destinationTable.create();

        LOGGER.info("Starting replication using stream <{}>", logfileIdStream);

        while (logfileIdStream.hasNext()) {
            final long blockStart = System.nanoTime();
            final Block block = logfileIdStream.next();
            final long lastId = databaseClient.replicateRangeAndReturnLastId(block, destinationTable);
            final LastIdSavedToFile lastIdSavedToFile = new LastIdSavedToFile(lastId);
            lastIdSavedToFile.save();

            // logging
            final long blockEnd = System.nanoTime();
            LOGGER.info("Replication took <{}>ms", (blockEnd - blockStart) / 1000000);
            final long processedIdCount = logfileIdStream.startId() + lastId;
            final long remainingIdCount = logfileIdStream.maxId() - lastId;
            LOGGER.info("Processed rows <{}>", processedIdCount);
            LOGGER.info("Remaining rows <{}>", remainingIdCount);
        }

        final long endTime = System.nanoTime(); // logging
        LOGGER.info("Replication took <{}>ms", (endTime - startTime) / 1000000);
    }

    @Override
    public void close() {
        LOGGER.info("Closing clients");
        databaseClient.close();
        hbaseClient.close();
    }
}
