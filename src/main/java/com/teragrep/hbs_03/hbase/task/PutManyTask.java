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
package com.teragrep.hbs_03.hbase.task;

import com.teragrep.hbs_03.HbsRuntimeException;
import com.teragrep.hbs_03.hbase.mutator.MutatorConfiguration;
import com.teragrep.hbs_03.hbase.mutator.MutatorParamsFromList;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public final class PutManyTask implements TableTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(PutManyTask.class);

    private final List<Put> puts;
    private final MutatorConfiguration configuration;

    public PutManyTask(final List<Put> puts) {
        this(puts, 1, false);
    }

    public PutManyTask(final List<Put> puts, final double overheadSize) {
        this(puts, overheadSize, true);
    }

    public PutManyTask(final List<Put> puts, final double overheadSize, final boolean useDynamicBuffer) {
        this(puts, new MutatorConfiguration(useDynamicBuffer, overheadSize));
    }

    public PutManyTask(final List<Put> puts, final MutatorConfiguration configuration) {
        this.puts = puts;
        this.configuration = configuration;
    }

    @Override
    public boolean work(final TableName tableName, final Connection tableConnection) {
        final BufferedMutatorParams params = new MutatorParamsFromList(puts, tableName, configuration).value();

        try (final BufferedMutator mutator = tableConnection.getBufferedMutator(params)) {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Putting <{}> objects", puts.size());
            }

            try {
                mutator.mutate(puts);
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

        return true;
    }

    @Override
    public String toString() {
        return "Puts: size <" + puts.size() + ">";
    }
}
