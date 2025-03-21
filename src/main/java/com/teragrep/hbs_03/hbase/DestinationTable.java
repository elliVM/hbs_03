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
package com.teragrep.hbs_03.hbase;

import com.teragrep.hbs_03.HbsRuntimeException;
import com.teragrep.hbs_03.hbase.task.TableTask;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HBase table that is the destination of the migration data. Has two column families: META that contains teragrep
 * metadata and BLOOM that contains bloomfilter data generated for the metadata.
 */
public final class DestinationTable implements HBaseTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DestinationTable.class);

    private final Connection connection;
    private final DestinationTableDescription tableDescriptor;

    public DestinationTable(final Connection connection) {
        this(connection, TableName.valueOf("logfile"));
    }

    public DestinationTable(final Connection connection, final TableName name) {
        this(connection, new DestinationTableDescription(name));
    }

    public DestinationTable(final Connection connection, final DestinationTableDescription tableDescriptor) {
        this.connection = connection;
        this.tableDescriptor = tableDescriptor;
    }

    @Override
    public void create() {
        final TableName name = tableDescriptor.name();
        try (final Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(name)) {
                final TableDescriptor descriptor = tableDescriptor.describe();
                admin.createTable(descriptor);
                LOGGER.debug("Created <{}> table to HBase", name);
            }
            else {
                LOGGER.debug("Table <{}> already exists, skipping creation", name);
            }
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

    @Override
    public void drop() {
        final TableName name = tableDescriptor.name();
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

    @Override
    public void workTask(final TableTask task) {
        final TableName name = tableDescriptor.name();
        final boolean finished = task.work(name, connection);
        LOGGER.info("Worked task <{}>, status: finished=<{}>", task, finished);
    }

}
