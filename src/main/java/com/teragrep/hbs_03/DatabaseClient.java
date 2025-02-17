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
import org.jooq.Record20;
import org.jooq.Record21;
import org.jooq.Result;
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

    public DatabaseClient(final DSLContext ctx, final Connection connection) {
        this(ctx, connection, 5000);
    }

    public DatabaseClient(final DSLContext ctx, final Connection connection, final int fetchSize) {
        this.ctx = ctx;
        this.connection = connection;
        this.fetchSize = fetchSize;
    }

    public int migrateForDate(final Date day, final LogfileTable hBaseTable) {
        LOGGER.debug("migrateForDate called with day <{}> and batch size <{}>", day, fetchSize);
        final long start = System.nanoTime();

        final LogfileTableFlatDayQuery logfileTableFlatDayQuery = new LogfileTableFlatDayQuery(ctx, day, fetchSize);
        final List<MetaRow> hbaseRows = new ArrayList<>(fetchSize);

        int totalRows = 0;
        try (final Cursor<Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>> cursor = logfileTableFlatDayQuery.asCursor()) {
            while (cursor.hasNext()) {
                long cursorStart = System.nanoTime();
                final Result<Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long>> nextResult = cursor.fetchNext(fetchSize);

                for (final Record21<ULong, Date, Date, String, String, String, String, String, Timestamp, ULong, String, String, String, String, String, String, ULong, UInteger, String, String, Long> row : nextResult) {
                    hbaseRows.add(new MetaRow(row));
                }

                if (hbaseRows.size() == 1) {
                    hBaseTable.put(hbaseRows.get(0).put());
                } else {
                    hBaseTable.putAll(hbaseRows);
                }
                totalRows = totalRows + hbaseRows.size();
                hbaseRows.clear();

                long cursorEnd = System.nanoTime();
                LOGGER.debug("migrateForDate cursor batch took <{}>ms", ((cursorEnd - cursorStart) / 1000000));
            }
        }
        final long end = System.nanoTime();
        LOGGER.debug("migrateForDate() took <{}>ms", ((end - start) / 1000000));
        return totalRows;
    }

    @Override
    public void close() {
        try {
            LOGGER.debug("Closing connection");
            connection.close();
        } catch (final SQLException e) {
            throw new HbsRuntimeException("Error closing connection", e);
        }
    }

    public List<String> grants() {
        final String grantsSQL = "SHOW GRANTS FOR CURRENT_USER()";
        final Result<Record> grants = ctx.fetch(grantsSQL);
        final List<String> grantsList = grants.getValues(0, String.class);
        LOGGER.info("Grants for current user <[{}]>", grantsList);
        return grantsList;
    }
}
