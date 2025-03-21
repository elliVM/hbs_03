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
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Calculates buffer size from a row list by checking the size of the first object, multiplying it by the size of the
 * list and finally by the multiplier. Size will be between min and max sizes. Default (2MB-64MB)
 */
public final class BufferSizeFromRowList implements Source<Long> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferSizeFromRowList.class);

    private final List<Put> puts;
    private final double multiplier;
    private final long minSize;
    private final long maxSize;

    /** Default between 2MB and 64MB with overhead multiplier of 2.0 */
    public BufferSizeFromRowList(final List<Put> puts) {
        this(puts, 2.0, (2 * 1024 * 1024), (64L * 1024L * 1024L));
    }

    public BufferSizeFromRowList(final List<Put> puts, final double multiplier) {
        this(puts, multiplier, (2 * 1024 * 1024), (64L * 1024L * 1024L));

    }

    public BufferSizeFromRowList(
            final List<Put> puts,
            final double multiplier,
            final long minSize,
            final long maxSize
    ) {
        this.puts = puts;
        this.multiplier = multiplier;
        this.minSize = minSize;
        this.maxSize = maxSize;
    }

    /**
     * @return Average value of Put.heapSize() in lists, between bound lower and upper limits
     */
    @Override
    public Long value() {
        // average of puts or 0 if not calculable like an empty list
        final double averagePutSize = puts.stream().mapToDouble(Put::heapSize).average().orElse(0);

        // list size * (average put * multiplier)
        final long estimatedBatchSize = Math.round(puts.size() * (averagePutSize * multiplier));

        if (estimatedBatchSize > maxSize) {
            LOGGER.warn("Estimate exceeded maximum size of <{}>, estimate was <{}>", maxSize, estimatedBatchSize);
        }

        final long boundSize = Math.min(Math.max(estimatedBatchSize, minSize), maxSize);
        LOGGER.debug("Calculated size <{}>", boundSize);
        return boundSize;
    }
}
