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

import org.jooq.CreateIndexIncludeStep;
import org.jooq.CreateTableConstraintStep;
import org.jooq.DSLContext;
import org.jooq.DropTableStep;
import org.jooq.Field;
import org.jooq.Index;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TruncateIdentityStep;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class SQLTempTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLTempTable.class);

    private final DSLContext ctx;
    private final String name;

    public SQLTempTable(final DSLContext ctx) {
        this(ctx, "archive_temporary_table");
    }

    public SQLTempTable(final DSLContext ctx, final String name) {
        this.ctx = ctx;
        this.name = name;
    }

    public Table<Record> table() {
        return DSL.table(DSL.name(name));
    }

    public void truncate() {
        try (final TruncateIdentityStep<Record> truncateStep = ctx.truncate(table())) {
            LOGGER.info("Truncating temporary table <{}>", truncateStep);
            truncateStep.execute();
        }
    }

    private void dropIfExists() {
        try (final DropTableStep dropQuery = ctx.dropTemporaryTableIfExists(table())) {
            LOGGER.info("Dropping temporary table if exists <{}>", dropQuery);
            dropQuery.execute();
        }
    }

    public void create() {
        final Field<ULong> idField = DSL.field(DSL.name(name, "id"), ULong.class);
        final Field<Integer> epochField = DSL.field(DSL.name(name, "epoch"), SQLDataType.INTEGER.nullable(false));

        final Field<?>[] columns = {
                idField,
                epochField,
                DSL.field(DSL.name(name, "expiration"), SQLDataType.DATE.nullable(false)),
                DSL.field(DSL.name(name, "path"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "name"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "archived"), Timestamp.class),
                DSL.field(DSL.name(name, "checksum"), SQLDataType.CHAR(64).nullable(false)),
                DSL.field(DSL.name(name, "etag"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "logtag"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "size"), ULong.class),
                DSL.field(DSL.name(name, "meta"), SQLDataType.JSON.nullable(true)),
                DSL.field(DSL.name(name, "source"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "category"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "bucket"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "host"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "stream_tag"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "log_group"), SQLDataType.VARCHAR(255).nullable(false)),
                DSL.field(DSL.name(name, "directory"), SQLDataType.VARCHAR(255).nullable(false))
        };
        dropIfExists();
        final Index timeIndex = DSL.index(DSL.name("ix_logtime"));
        try (
                final CreateTableConstraintStep createQuery = ctx.createTemporaryTable(name).columns(columns).constraint(DSL.constraint("pk").primaryKey(idField)); final CreateIndexIncludeStep indexStep = ctx.createIndex(timeIndex).on(table(), epochField);
        ) {
            LOGGER.info("Creating temp table <{}>", createQuery);
            createQuery.execute();
            LOGGER.info("Creating index on temp table epoch field <{}>", indexStep);
            indexStep.execute();
        }
    }
}
