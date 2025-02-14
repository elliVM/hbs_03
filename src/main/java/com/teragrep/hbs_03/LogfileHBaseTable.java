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
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class LogfileHBaseTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogfileHBaseTable.class);
    private final Connection connection;
    private final TableName tableName;

    public LogfileHBaseTable(final Connection connection) {
        this(connection, TableName.valueOf("logfile"));
    }

    public LogfileHBaseTable(final Connection connection, final String tableName) {
        this(connection, TableName.valueOf(tableName));
    }

    public LogfileHBaseTable(final Connection connection, final TableName tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    public void create() {
        try (final Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(tableName)) {
                final TableDescriptor descriptor = TableDescriptorBuilder
                        .newBuilder(tableName)
                        .setColumnFamilies(columnFamilyDescriptors())
                        .build();
                admin.createTable(descriptor);
                LOGGER.info("Created Logfile table to HBase");
            }
        }
        catch (final MasterNotRunningException e) {
            throw new RuntimeException("Master was not running: " + e.getMessage());
        }
        catch (final TableExistsException e) {
            throw new RuntimeException("Logfile table already exists: " + e.getMessage());
        }
        catch (final IllegalArgumentException e) {
            throw new RuntimeException(tableName + " restricted: " + e.getMessage());
        }
        catch (final IOException e) {
            throw new RuntimeException("Error creating logfile table: " + e.getMessage());
        }
    }

    public void delete() {
        try (final Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                if (!admin.isTableDisabled(tableName)) {
                    admin.disableTable(tableName);
                }
                LOGGER.info("Disabled table <{}>", tableName);
                admin.deleteTable(tableName);
                LOGGER.info("Deleted table <{}>", tableName);
            }
        }
        catch (final IOException e) {
            throw new RuntimeException("Error deleting table: " + e);
        }
    }

    public List<Result> scan(final Scan scan) {
        final List<Result> results = new ArrayList<>();
        try (final Table table = connection.getTable(tableName)) {
            try (final ResultScanner scanner = table.getScanner(scan)) {
                for (final Result result : scanner) {
                    results.add(result);
                }
            }
            catch (final IOException e) {
                throw new RuntimeException("Error getting ResultScanner: " + e);
            }
        }
        catch (final IOException e) {
            throw new RuntimeException("ERror getting table from connection: " + e);
        }
        return Collections.unmodifiableList(results);
    }

    public void put(final Put put) {
        try (final Table table = connection.getTable(tableName)) {
            table.put(put);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Put <{}> into database", put);
            }
        }
        catch (final IOException e) {
            throw new RuntimeException("Error writing files to table: " + e.getMessage());
        }
    }

    /**
     * Uses BufferedMutator to mutate puts and flush
     *
     * @param rows List of puts that are added to the BufferedMutator
     */
    public void putAll(final List<HBaseRow> rows) {
        final BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        try (final BufferedMutator mutator = connection.getBufferedMutator(params)) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Putting <{}> objects", rows.size());
            }
            try {
                final List<Put> putList = rows.stream().map(HBaseRow::put).collect(Collectors.toList());
                mutator.mutate(putList);
            }
            catch (final IOException e) {
                LOGGER.error("Error executing mutator <{}>", mutator);
            }
            mutator.flush();
        }
        catch (final IOException e) {
            throw new RuntimeException("Error writing files to table: " + e.getMessage());
        }
    }

    private List<ColumnFamilyDescriptor> columnFamilyDescriptors() {
        return Collections
                .unmodifiableList(Arrays.asList(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("meta")).build(), ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("bloom")).build()));
    }
}
