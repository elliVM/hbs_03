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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * HBase table that is the destination of the migration data
 */
public final class DestinationTable implements HBaseTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DestinationTable.class);
    private final Connection connection;
    private final DestinationTableDescription tableDescriptor;
    private final TableName name;
    private final ConfiguredMutator mutator;

    public DestinationTable(final Connection connection, final TableName name) {
        this(connection, name, new DestinationTableDescription(name), new ConfiguredMutator(name, false));
    }

    public DestinationTable(final Connection connection, final TableName name, final boolean useDynamicBufferSize) {
        this(connection, name, new DestinationTableDescription(name), new ConfiguredMutator(name, useDynamicBufferSize));
    }

    public DestinationTable(final Connection connection, final TableName name, final double overheadSize) {
        this(connection, name, new DestinationTableDescription(name), new ConfiguredMutator(name, overheadSize));
    }

    public DestinationTable(
            final Connection connection,
            final TableName name,
            final boolean useDynamicBuffer,
            final double overheadSize
    ) {
        this(
                connection,
                name,
                new DestinationTableDescription(name),
                new ConfiguredMutator(name, useDynamicBuffer, overheadSize)
        );
    }

    public DestinationTable(
            final Connection connection,
            final TableName name,
            final DestinationTableDescription tableDescriptor,
            final ConfiguredMutator mutator
    ) {

        this.connection = connection;
        this.name = name;
        this.tableDescriptor = tableDescriptor;
        this.mutator = mutator;
    }

    public void create() {
        try (final Admin admin = connection.getAdmin()) {
            final TableDescriptor descriptor = tableDescriptor.description();
            admin.createTable(descriptor);
            LOGGER.debug("Created <{}> table to HBase", name);
        }
        catch (final MasterNotRunningException e) {
            throw new HbsRuntimeException("Master was not running", e);
        }
        catch (final IllegalArgumentException e) {
            throw new HbsRuntimeException("Table name was restricted", e);
        }
        catch (final IOException e) {
            throw new HbsRuntimeException("Error creating logfile table", e);
        }
    }

    public void createIfNotExists() {
        try (final Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(name)) {
                create();
            }
            else {
                LOGGER.debug("Table <{}> already exists, skipping creation", name);
            }
        }
        catch (IOException e) {
            throw new HbsRuntimeException("Error creating logfile table", e);
        }
    }

    public void delete() {
        try (final Admin admin = connection.getAdmin()) {
            if (admin.tableExists(name)) {
                if (!admin.isTableDisabled(name)) {
                    admin.disableTable(name);
                    LOGGER.debug("Disabled table <{}>", name);
                }
                admin.deleteTable(name);
                LOGGER.debug("Deleted table <{}>", name);
            }
        }
        catch (final IOException e) {
            throw new HbsRuntimeException("Error deleting table", e);
        }
    }

    public List<Result> scan(final Scan scan) {
        final List<Result> results = new ArrayList<>();
        try (final Table table = connection.getTable(name)) {
            try (final ResultScanner scanner = table.getScanner(scan)) {
                for (final Result result : scanner) {
                    results.add(result);
                }
            }
            catch (final IOException e) {
                throw new HbsRuntimeException("Error getting scanner", e);
            }
        }
        catch (final IOException e) {
            throw new HbsRuntimeException("Error getting table from connection", e);
        }
        return Collections.unmodifiableList(results);
    }

    public long put(final Put put) {
        try (final Table table = connection.getTable(name)) {
            table.put(put);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Put <{}> into database", put);
            }
        }
        catch (final IOException e) {
            throw new HbsRuntimeException("Error writing files to table", e);
        }
        return 1L;
    }

    public long putAll(final List<Row> rows) {
        final AtomicLong successfulInserts = new AtomicLong(0); // mutate() is asynchronous
        final BufferedMutatorParams params = mutator.paramsForRows(rows);
        try (final BufferedMutator mutator = connection.getBufferedMutator(params)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Putting <{}> objects", rows.size());
            }
            try {
                final List<Put> putList = rows.stream().map(Row::put).collect(Collectors.toList());
                mutator.mutate(putList);
                successfulInserts.addAndGet(rows.size());
                mutator.flush();
            }
            catch (final IOException e) {
                LOGGER.error("Error executing mutator <{}>", mutator);
                throw new HbsRuntimeException("Error executing mutator", e);
            }
        }
        catch (final IOException e) {
            throw new HbsRuntimeException("Error creating BufferedMutator", e);
        }

        return successfulInserts.get();
    }
}
