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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDate;

/**
 * SQL Teragrep metadata to HBase between set date range
 */
public final class ReplicateDateRange implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicateDateRange.class);

    private final Date start;
    private final Date end;
    private final DatabaseClient databaseClient;
    private final HBaseClient hbaseClient;

    public ReplicateDateRange(
            final Date start,
            final Date end,
            final DatabaseClient databaseClient,
            final HBaseClient hbaseClient
    ) {
        this.start = start;
        this.end = end;
        this.databaseClient = databaseClient;
        this.hbaseClient = hbaseClient;
    }

    public void start() {
        final long startTime = System.nanoTime(); // logging

        final HBaseTable destinationTable = hbaseClient.destinationTable();
        destinationTable.createIfNotExists();

        LOGGER.info("Replication started from <{}> to <{}>", start, end);
        final LocalDate startDate = start.toLocalDate();
        final LocalDate endDate = end.toLocalDate();
        long totalRows = 0;

        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            totalRows += databaseClient.replicateDate(Date.valueOf(date), destinationTable);
        }

        final long endTime = System.nanoTime(); // logging
        LOGGER.info("Total rows replicated <{}>", totalRows);
        LOGGER.info("Replication took <{}>ms", (endTime - startTime) / 1000000);
    }

    @Override
    public void close() {
        LOGGER.info("Closing clients");
        databaseClient.close();
        hbaseClient.close();
    }
}
