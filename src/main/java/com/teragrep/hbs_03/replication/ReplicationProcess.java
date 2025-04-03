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
import com.teragrep.hbs_03.hbase.Row;
import com.teragrep.hbs_03.hbase.mutator.MutatorConfiguration;
import com.teragrep.hbs_03.hbase.task.PutRowsTask;
import com.teragrep.hbs_03.sql.DatabaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Replicate SQL rows to HBase between the range for the BlockRangeStream
 */
public final class ReplicationProcess implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationProcess.class);

    private final DatabaseClient databaseClient;
    private final HBaseClient hbaseClient;
    private final BlockRangeStream blockRangeStream;
    private final MutatorConfiguration mutatorConfiguration;

    public ReplicationProcess(
            final DatabaseClient databaseClient,
            final HBaseClient hbaseClient,
            final BlockRangeStream blockRangeStream
    ) {
        this(databaseClient, hbaseClient, blockRangeStream, new MutatorConfiguration(false));
    }

    public ReplicationProcess(
            final DatabaseClient databaseClient,
            final HBaseClient hbaseClient,
            final BlockRangeStream blockRangeStream,
            final MutatorConfiguration mutatorConfiguration
    ) {
        this.databaseClient = databaseClient;
        this.hbaseClient = hbaseClient;
        this.blockRangeStream = blockRangeStream;
        this.mutatorConfiguration = mutatorConfiguration;
    }

    public void replicate() {
        final long startTime = System.nanoTime(); // logging

        final HBaseTable destinationTable = hbaseClient.destinationTable();
        destinationTable.create();

        LOGGER.info("Starting replication using stream <{}>", blockRangeStream);

        while (blockRangeStream.hasNext()) {
            final long blockStartTime = System.nanoTime();

            final Block block = blockRangeStream.next();

            final List<Row> rangeRowResults = databaseClient.rangeResults(block);
            destinationTable.workTask(new PutRowsTask(rangeRowResults, mutatorConfiguration));

            final long maxIdFromResults = new RowListMaxId(rangeRowResults).value();
            final LastIdSavedToFile lastIdSavedToFile = new LastIdSavedToFile(maxIdFromResults);
            lastIdSavedToFile.save();

            // logging
            final long blockEndTime = System.nanoTime();
            LOGGER.info("Batch replication took <{}>ms", (blockEndTime - blockStartTime) / 1000000);
            final long processedIdCount = blockRangeStream.startId() + maxIdFromResults;
            final long remainingIdCount = blockRangeStream.maxId() - maxIdFromResults;
            LOGGER.info("Total processed rows <{}>", processedIdCount);
            LOGGER.info("Total remaining rows <{}>", remainingIdCount);
        }

        final long endTime = System.nanoTime(); // logging
        LOGGER.info("Replication finished, total time <{}>ms", (endTime - startTime) / 1000000);
    }

    @Override
    public void close() {
        LOGGER.info("Closing clients");
        databaseClient.close();
        hbaseClient.close();

    }
}
