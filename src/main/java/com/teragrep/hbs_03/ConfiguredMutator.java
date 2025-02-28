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
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class ConfiguredMutator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredMutator.class);

    private final TableName name;
    private final double overheadMultiplier;
    private final boolean userDynamicBuffer;

    public ConfiguredMutator(final TableName name) {
        this(name, false, 1);
    }

    public ConfiguredMutator(final TableName name, final double overheadMultiplier) {
        this(name, true, overheadMultiplier);
    }

    public ConfiguredMutator(final TableName name, final boolean userDynamicBuffer) {
        this(name, userDynamicBuffer, 2.0);
    }

    public ConfiguredMutator(final TableName name, final boolean userDynamicBuffer, final double overheadMultiplier) {
        this.name = name;
        this.overheadMultiplier = overheadMultiplier;
        this.userDynamicBuffer = userDynamicBuffer;
    }

    public BufferedMutatorParams paramsForRows(final List<Row> rows) {
        final BufferedMutatorParams params;
        if (userDynamicBuffer) {
            final long dynamicBufferSize = calculatedBufferSize(rows);
            LOGGER.debug("Using dynamic buffer size <{}>MB", (dynamicBufferSize / (1024 * 1024)));
            params = new BufferedMutatorParams(name).listener(exceptionListener()).writeBufferSize(dynamicBufferSize);
        }
        else {
            params = defaultBuffer();
        }
        return params;
    }

    public long calculatedBufferSize(final List<Row> rows) {
        final int multiplierUpperLimit = 5;
        if (overheadMultiplier < 1 || overheadMultiplier > multiplierUpperLimit) {
            throw new HbsRuntimeException(
                    "Overhead multiplier was not between 1-5",
                    new IllegalAccessError("Illegal overhead multiplier <" + overheadMultiplier + ">")
            );
        }
        final long numberOfRows = rows.size();
        final long averageSize = rows.get(0).put().heapSize();
        final long estimatedBatchSize = Math.round(numberOfRows * (averageSize * overheadMultiplier));
        final long maxBufferSize = 64L * 1024 * 1024; // 64MB max size

        if (estimatedBatchSize > maxBufferSize) {
            LOGGER.warn("Estimate exceeded maximum size of 64MB, estimate was <{}>", estimatedBatchSize);
        }

        final long estimateOrMinimumSize;
        final long twoMegaBytes = 2 * 1024 * 1024;
        if (estimatedBatchSize < twoMegaBytes) {
            LOGGER.debug("Estimate <{}> smaller than minimum size, using minimum 2MB buffer", estimatedBatchSize);
            estimateOrMinimumSize = twoMegaBytes;
        }
        else {
            estimateOrMinimumSize = estimatedBatchSize;
        }

        return Math.min(estimateOrMinimumSize, maxBufferSize);
    }

    public BufferedMutatorParams defaultBuffer() {
        return new BufferedMutatorParams(name).listener(exceptionListener());
    }

    private BufferedMutator.ExceptionListener exceptionListener() {
        return (e, mutator) -> {
            LOGGER.error("Failed mutation <{}>, number of exceptions <{}>", e.getMessage(), e.getNumExceptions(), e);
        };
    }
}
