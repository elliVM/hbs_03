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
package com.teragrep.hbs_03.hbase.mutator;

import com.teragrep.hbs_03.Source;
import com.teragrep.hbs_03.hbase.BytesInMB;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class MutatorParamsSourceFromList implements Source<BufferedMutatorParams> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MutatorParamsSourceFromList.class);
    private final TableName name;
    private final BufferSizeFromRowList bufferSizeFromRowList;

    public MutatorParamsSourceFromList(final TableName name, final List<Put> puts) {
        this(name, new BufferSizeFromRowList(puts, 2.0));
    }

    public MutatorParamsSourceFromList(final TableName name, final List<Put> puts, final double overheadMultiplier) {
        this(name, new BufferSizeFromRowList(puts, overheadMultiplier));
    }

    public MutatorParamsSourceFromList(final TableName name, final BufferSizeFromRowList bufferSizeFromRowList) {
        this.name = name;
        this.bufferSizeFromRowList = bufferSizeFromRowList;
    }

    @Override
    public BufferedMutatorParams value() {
        final BufferedMutatorParams params;
        final long dynamicBufferSize = bufferSizeFromRowList.value();
        final BytesInMB sizeInMB = new BytesInMB(dynamicBufferSize);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Using dynamic buffer size <{}>MB", sizeInMB.asLong());
        }
        params = new BufferedMutatorParams(name).listener(exceptionListener()).writeBufferSize(dynamicBufferSize);

        return params;
    }

    private BufferedMutator.ExceptionListener exceptionListener() {
        return (e, mutator) -> {
            LOGGER.error("Failed mutation <{}>, number of exceptions <{}>", e.getMessage(), e.getNumExceptions(), e);
        };
    }

}
