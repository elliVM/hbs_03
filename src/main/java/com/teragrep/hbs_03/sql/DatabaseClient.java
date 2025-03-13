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
package com.teragrep.hbs_03.sql;

import com.teragrep.hbs_03.HbsRuntimeException;
import com.teragrep.hbs_03.hbase.HBaseTable;
import com.teragrep.hbs_03.hbase.Row;
import com.teragrep.hbs_03.replication.Block;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSelectStep;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;

public final class DatabaseClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClient.class);

    private final DSLContext ctx;
    private final Connection connection;

    public DatabaseClient(final Connection conn, final Settings settings) {
        this(DSL.using(conn, SQLDialect.MYSQL, settings), conn);
    }

    public DatabaseClient(final DSLContext ctx, final Connection connection) {
        this.ctx = ctx;
        this.connection = connection;
    }

    public ULong lastId() {
        final ULong maxId;
        try (
                final SelectSelectStep<Record1<ULong>> selectMaxOrDefaultStep = ctx
                        .select(DSL.coalesce(DSL.max(JOURNALDB.LOGFILE.ID), DSL.val(ULong.valueOf(2L))));
                final SelectJoinStep<Record1<ULong>> selectMaxOrDefaultFromStep = selectMaxOrDefaultStep
                        .from(JOURNALDB.LOGFILE)
        ) {
            maxId = selectMaxOrDefaultFromStep.fetchOneInto(ULong.class);
        }
        LOGGER.trace("MAX(LOGFILE.ID)=<{}>", maxId);
        return maxId;
    }

    public ULong firstAvailableId() {
        final ULong minIdLongValue;
        try (
                final SelectSelectStep<Record1<ULong>> selectMinOrZero = ctx
                        .select(DSL.coalesce(DSL.min(JOURNALDB.LOGFILE.ID), DSL.val(ULong.valueOf(0L))));
                final SelectJoinStep<Record1<ULong>> selectMinOrZeroFrom = selectMinOrZero.from(JOURNALDB.LOGFILE);
        ) {
            minIdLongValue = selectMinOrZeroFrom.fetchOneInto(ULong.class);
        }
        return minIdLongValue;
    }

    public long replicateRangeAndReturnLastId(final Block block, final HBaseTable destinationTable) {

        final LogfileTableFlatQuery logfileTableFlatQuery = new LogfileTableFlatQuery(ctx, block.start(), block.end());

        final List<Row> rowList = logfileTableFlatQuery.resultRowList();

        destinationTable.putAll(rowList);

        long maxIdInList = 0;
        for (final Row row : rowList) {
            final long rowIdValue = row.id().longValue();
            if (rowIdValue > maxIdInList) {
                maxIdInList = rowIdValue;
            }
        }

        return maxIdInList;
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
