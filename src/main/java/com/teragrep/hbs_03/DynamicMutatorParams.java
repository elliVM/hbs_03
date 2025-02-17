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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicMutatorParams {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicMutatorParams.class);

    private final TableName name;
    private final int numberOfPuts;
    private final long averageSize;

    public DynamicMutatorParams(final String tableName, final int numberOfPuts) {
        this(TableName.valueOf(tableName), numberOfPuts);
    }

    // the average Put heap size was 2800, double for overhead
    public DynamicMutatorParams(final TableName name, final int numberOfPuts) {
        this(name, numberOfPuts, (2800L * 2));
    }

    public DynamicMutatorParams(final String tableName, final int numberOfPuts, final MetaRow exampleRow) {
        this(TableName.valueOf(tableName), numberOfPuts, exampleRow.put().heapSize());
    }

    public DynamicMutatorParams(final String tableName, final int numberOfPuts, final Put examplePut) {
        this(TableName.valueOf(tableName), numberOfPuts, examplePut.heapSize());
    }

    public DynamicMutatorParams(final String tableName, final int numberOfPuts, final long averageSize) {
        this(TableName.valueOf(tableName), numberOfPuts, averageSize);
    }

    public DynamicMutatorParams(final TableName name, final int numberOfPuts, final MetaRow exampleRow) {
        this(name, numberOfPuts, exampleRow.put().heapSize());
    }

    public DynamicMutatorParams(final TableName name, final int numberOfPuts, final Put examplePut) {
        this(name, numberOfPuts, examplePut.heapSize());
    }

    public DynamicMutatorParams(final TableName name, final int numberOfPuts, final long averageSize) {
        this.name = name;
        this.numberOfPuts = numberOfPuts;
        this.averageSize = averageSize;
    }

    public BufferedMutatorParams params() {
        final long adjustedBufferSize = bufferSize();
        LOGGER.debug("using adjusted buffer size <{}>", adjustedBufferSize);
        return new BufferedMutatorParams(name)
                .listener((e, mutator) -> LOGGER.error("Error during mutation: <{}>", e.getMessage(), e))
                .writeBufferSize(bufferSize());
    }

    public long bufferSize() {
        final long estimatedBatchSize = numberOfPuts * (averageSize * 2); // double of average size for overhead
        final long maxBufferSize = 64L * 1024 * 1024; // 64MB max size
        if (estimatedBatchSize > maxBufferSize) {
            LOGGER.warn("Estimate exceeded maximum size of 64MB, estimate was <{}>", estimatedBatchSize);
        }
        return Math.min(estimatedBatchSize, maxBufferSize);
    }
}
