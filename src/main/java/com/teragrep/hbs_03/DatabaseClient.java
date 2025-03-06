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

import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record21;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public final class DatabaseClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClient.class);

    private final DSLContext ctx;
    private final Connection connection;
    private final int fetchSize;

    public DatabaseClient(final Connection conn, final Settings settings) {
        this(DSL.using(conn, SQLDialect.MYSQL, settings), conn);
    }

    public DatabaseClient(final Connection conn, final Settings settings, final int fetchSize) {
        this(DSL.using(conn, SQLDialect.MYSQL, settings), conn, fetchSize);
    }

    public DatabaseClient(final DSLContext ctx, final Connection connection) {
        this(ctx, connection, 5000);
    }

    public DatabaseClient(final DSLContext ctx, final Connection connection, final int fetchSize) {
        this.ctx = ctx;
        this.connection = connection;
        this.fetchSize = fetchSize;
    }

    public long replicateDate(final Date day, final HBaseTable destinationTable) {
        final long start = System.nanoTime(); // logging

        LOGGER.debug("replicateDate() called with day <{}> and batch size <{}>", day, fetchSize);

        final LogfileTableFlatQuery logfileTableFlatQuery = new LogfileTableFlatQuery(ctx, day, fetchSize);
        final List<Row> cursorBatchRowList = new ArrayList<>(fetchSize);

        long totalQueriedRows = 0;
        long totalInsertedRows = 0;
        try (
                final Cursor<Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>> cursor = logfileTableFlatQuery
                        .asCursor()
        ) {
            while (cursor.hasNext()) {
                final long batchStart = System.nanoTime(); // logging

                final Result<Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>> cursorBatch = cursor
                        .fetchNext(fetchSize);

                for (
                    final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> record : cursorBatch
                ) {
                    final Row row = new MetaRow(record);
                    cursorBatchRowList.add(row);
                }

                if (cursorBatchRowList.size() == 1) {
                    totalInsertedRows += destinationTable.put(cursorBatchRowList.get(0).put());
                }
                else {
                    totalInsertedRows += destinationTable.putAll(cursorBatchRowList);
                }
                totalQueriedRows += cursorBatchRowList.size();
                cursorBatchRowList.clear();

                final long batchEnd = System.nanoTime(); // logging
                LOGGER.debug("replicateDate() batch took <{}>ms", ((batchEnd - batchStart) / 1000000));
            }
        }
        final long end = System.nanoTime(); // logging
        LOGGER.debug("replicateDate() took <{}>ms", ((end - start) / 1000000));

        if (totalQueriedRows != totalInsertedRows) {
            LOGGER
                    .error(
                            "Miss matching size of extracted SQL rows <{}> and inserted HBase rows <{}>",
                            totalQueriedRows, totalInsertedRows
                    );
        }

        return totalInsertedRows;
    }

    @Override
    public void close() {
        try {
            LOGGER.debug("Closing connection");
            connection.close();
        }
        catch (final SQLException e) {
            throw new HbsRuntimeException("Error closing connection", e);
        }
    }

    public List<String> grants() {
        final String grantsSQL = "SHOW GRANTS FOR CURRENT_USER()";
        final Result<Record> grants = ctx.fetch(grantsSQL);
        final List<String> grantsList = grants.getValues(0, String.class);
        LOGGER.debug("Fetched grants for current user <{}>", grantsList);
        return grantsList;
    }
}
