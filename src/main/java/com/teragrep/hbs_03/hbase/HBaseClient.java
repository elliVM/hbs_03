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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Client to establish connection to HBase and provide the DestinationTable */
public final class HBaseClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseClient.class);

    private final LazyConnection lazyConnection;
    private final TableName name;
    private final boolean useDynamicBufferSize;
    private final double overheadMultiplier;

    public HBaseClient(final Configuration configuration, final String name) {
        this(new LazyConnection(configuration, 1), TableName.valueOf(name), false, 2.0);
    }

    public HBaseClient(final Configuration configuration, final String name, final boolean useDynamicBufferSize) {
        this(new LazyConnection(configuration, 1), TableName.valueOf(name), useDynamicBufferSize, 2.0);
    }

    public HBaseClient(
            final LazyConnection lazyConnection,
            final TableName name,
            final boolean useDynamicBufferSize,
            final double overheadMultiplier
    ) {
        this.lazyConnection = lazyConnection;
        this.name = name;
        this.useDynamicBufferSize = useDynamicBufferSize;
        this.overheadMultiplier = overheadMultiplier;
    }

    /**
     * @return
     */
    private Connection connection() {
        return lazyConnection.value();
    }

    @Override
    public void close() {
        LOGGER.debug("Closing connection");
        try {
            connection().close();
        }
        catch (final IOException e) {
            throw new HbsRuntimeException("Error closing connection", e);
        }
    }

    public HBaseTable destinationTable() {
        return new DestinationTable(connection(), name, useDynamicBufferSize, overheadMultiplier);
    }
}
