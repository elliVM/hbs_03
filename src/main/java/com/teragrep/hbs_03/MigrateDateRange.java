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
 *
 */
public class MigrateDateRange implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateDateRange.class);

    private final Date start;
    private final Date end;
    private final DatabaseClient metaSQLClient;
    private final HBaseClient hbaseClient;

    public MigrateDateRange(
            final Date start,
            final Date end,
            DatabaseClient metaSQLClient,
            final HBaseClient hbaseClient
    ) {
        this.start = start;
        this.end = end;
        this.metaSQLClient = metaSQLClient;
        this.hbaseClient = hbaseClient;
    }

    public void start() {
        long startTime = System.nanoTime();
        final LogfileTable logfileTable = hbaseClient.logfile();
        logfileTable.create();
        LOGGER.info("Replication started from <{}> to <{}>", start, end);
        final LocalDate endDate = end.toLocalDate();
        long totalRows = 0;
        LocalDate rollingDay = start.toLocalDate();
        while (rollingDay.isBefore(endDate)) {
            final Date date = Date.valueOf(rollingDay);
            final int rows = metaSQLClient.migrateForDate(date, logfileTable);
            totalRows += rows;
            LOGGER.info("Processing date <{}> affected <{}> row(s)", date, rows);
            rollingDay = rollingDay.plusDays(1);
        }
        long endTime = System.nanoTime();
        LOGGER.info("Total rows migrated <{}>", totalRows);
        LOGGER.info("Migration took <{}>ms", (endTime - startTime) / 1000000);
    }

    @Override
    public void close() {
        LOGGER.info("Closing clients");
        metaSQLClient.close();
        hbaseClient.close();
    }
}
