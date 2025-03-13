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

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
import org.jooq.SelectForUpdateStep;
import org.jooq.SelectSelectStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.teragrep.hbs_03.jooq.generated.journaldb.Journaldb.JOURNALDB;

/**
 * Queries a sized batch of LOGFILE.ID columns from a given start ID
 */
public final class LogfileTableIdBatchQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogfileTableIdBatchQuery.class);

    private final DSLContext ctx;
    private final ULong startId;
    private final ULong endId;

    public LogfileTableIdBatchQuery(final DSLContext ctx, final long startId, final long endId) {
        this(ctx, ULong.valueOf(startId), ULong.valueOf(endId));
    }

    public LogfileTableIdBatchQuery(final DSLContext ctx, final ULong startId, final ULong endId) {
        this.ctx = ctx;
        this.startId = startId;
        this.endId = endId;
    }

    private Table<Record> table() {
        return DSL.table(DSL.name("logfile_id_batch_table"));
    }

    /**
     * Use limit and offset to select a batch
     */
    public Table<Record1<ULong>> withLimitTable() {
        final Table<Record1<ULong>> idRangeTable;
        try (
                final SelectSelectStep<Record1<ULong>> selectId = ctx.select(JOURNALDB.LOGFILE.ID); final SelectForUpdateStep<Record1<ULong>> whereBetweenLimitStep = selectId.from(JOURNALDB.LOGFILE).limit(endId).offset(startId)
        ) {
            LOGGER.debug("Select with limit <{}>", whereBetweenLimitStep);
            idRangeTable = whereBetweenLimitStep.asTable(table());
        }
        return idRangeTable;
    }

    /**
     * Use where between to select a batch
     */
    public Table<Record1<ULong>> asTable() {
        final Condition idBetweenCondition = JOURNALDB.LOGFILE.ID.between(startId, endId);
        LOGGER.debug("Select from logfile where <{}>", idBetweenCondition);
        final Table<Record1<ULong>> idRangeTable;
        try (
                final SelectSelectStep<Record1<ULong>> selectId = ctx.select(JOURNALDB.LOGFILE.ID); final SelectConditionStep<Record1<ULong>> whereBetweenStep = selectId.from(JOURNALDB.LOGFILE).where(idBetweenCondition)
        ) {
            LOGGER.debug("Select id range <{}>", whereBetweenStep);
            idRangeTable = whereBetweenStep.asTable(table());
        }
        return idRangeTable;
    }

}
