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

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class LogfileTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogfileTable.class);
    private final Connection connection;
    private final LogfileTableTableDescriptor tableDescriptor;

    public LogfileTable(final Connection connection, final String tableName) {
        this(connection, new LogfileTableTableDescriptor(tableName));
    }

    public LogfileTable(final Connection connection, final LogfileTableTableDescriptor tableDescriptor) {
        this.connection = connection;
        this.tableDescriptor = tableDescriptor;
    }

    public void create() {
        final TableName name = tableDescriptor.name();
        try (final Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(name)) {
                final TableDescriptor descriptor = tableDescriptor.descriptor();
                admin.createTable(descriptor);
                LOGGER.debug("Created <{}> table to HBase", name);
            } else {
                LOGGER.debug("Table <{}> already exists, skipping creation", name);
            }
        } catch (final MasterNotRunningException e) {
            throw new HbsRuntimeException("Master war not running", e);
        } catch (final IllegalArgumentException e) {
            throw new HbsRuntimeException(name + " restricted", e);
        } catch (final IOException e) {
            throw new HbsRuntimeException("Error creating logfile table", e);
        }
    }

    public void delete() {
        final TableName name = tableDescriptor.name();
        try (final Admin admin = connection.getAdmin()) {
            if (admin.tableExists(name)) {
                if (!admin.isTableDisabled(name)) {
                    LOGGER.debug("Disabled table <{}>", name);
                    admin.disableTable(name);
                }
                admin.deleteTable(name);
                LOGGER.debug("Deleted table <{}>", name);
            }
        } catch (final IOException e) {
            throw new HbsRuntimeException("Error deleting table", e);
        }
    }

    public List<Result> scan(final Scan scan) {
        final TableName name = tableDescriptor.name();
        final List<Result> results = new ArrayList<>();
        try (final Table table = connection.getTable(name)) {
            try (final ResultScanner scanner = table.getScanner(scan)) {
                for (final Result result : scanner) {
                    results.add(result);
                }
            } catch (final IOException e) {
                throw new HbsRuntimeException("Error getting scanner", e);
            }
        } catch (final IOException e) {
            throw new HbsRuntimeException("Error getting table from connection", e);
        }
        return Collections.unmodifiableList(results);
    }

    public void put(final Put put) {
        final TableName name = tableDescriptor.name();
        try (final Table table = connection.getTable(name)) {
            table.put(put);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Put <{}> into database", put);
            }
        } catch (final IOException e) {
            throw new HbsRuntimeException("Error writing files to table", e);
        }
    }

    /**
     * Uses BufferedMutator to mutate puts and flush
     *
     * @param rows List of puts that are added to the BufferedMutator
     */
    public void putAll(final List<MetaRow> rows) {

        final TableName name = tableDescriptor.name();

        final BufferedMutatorParams defaultParams = new BufferedMutatorParams(name)
                .listener((e, mutator) -> LOGGER.error("Error during mutation: <{}>", e.getMessage(), e))
                .writeBufferSize(32 * 1024 * 1024);

        final MetaRow exampleRow = rows.get(0); // select one row used to batch size
        final BufferedMutatorParams params = new DynamicMutatorParams(name, rows.size(), exampleRow).params();
        try (final BufferedMutator mutator = connection.getBufferedMutator(defaultParams)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Putting <{}> objects", rows.size());
            }
            try {
                final List<Put> putList = rows.stream().map(MetaRow::put).collect(Collectors.toList());
                mutator.mutate(putList);
                mutator.flush();
            } catch (final IOException e) {
                LOGGER.error("Error executing mutator <{}>", mutator);
                throw new HbsRuntimeException("Error executing mutator", e);
            }
        } catch (final IOException e) {
            throw new HbsRuntimeException("Error creating BufferedMutator", e);
        }
    }
}
