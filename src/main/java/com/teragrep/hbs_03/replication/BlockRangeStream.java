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
package com.teragrep.hbs_03.replication;

import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class BlockRangeStream implements Iterator<Block> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockRangeStream.class);

    private Block lastBlock;
    private final long startId;
    private final long maxId;
    private final long maxBatchSize;

    public BlockRangeStream(final ULong startId, final ULong maxId, final long maxBatchSize) {
        this(startId.longValue(), maxId.longValue(), maxBatchSize, new BlockStub());
    }

    public BlockRangeStream(final long startId, final long maxId, final long maxBatchSize) {
        this(startId, maxId, maxBatchSize, new BlockStub());
    }

    public BlockRangeStream(final long startId, final long maxId, final long maxBatchSize, final Block lastBlock) {
        this.startId = startId;
        this.maxId = maxId;
        this.maxBatchSize = maxBatchSize;
        this.lastBlock = lastBlock;
    }

    public long startId() {
        return startId;
    }

    public long maxId() {
        return maxId;
    }

    @Override
    public boolean hasNext() {
        final boolean hasNext;
        if (lastBlock.isStub()) {
            hasNext = startId < maxId;
        }
        else {
            hasNext = lastBlock.end() < maxId;
        }
        return hasNext;
    }

    @Override
    public Block next() {
        final Block block;
        if (hasNext()) {
            final long lastEndId = lastBlock.isStub() ? startId : lastBlock.end(); // get last blocks end id or use start id if stub
            final long endId = Math.min((lastEndId + maxBatchSize), maxId); // use max id if calculater next block exceeds it
            block = new BlockValid(new BlockPositiveValues(new BlockImpl(lastEndId, endId)));
        }
        else {
            throw new NoSuchElementException("No next block available");
        }
        lastBlock = block;
        return block;
    }

    @Override
    public String toString() {
        return String
                .format("Stream of IDs between %s and %s, batches with maximum size of %s", startId, maxId, maxBatchSize);
    }
}
