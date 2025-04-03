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

import com.teragrep.hbs_03.hbase.Row;
import org.apache.hadoop.hbase.client.Put;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public final class BufferSizeFromRowListTest {

    final long twoMB = 2 * 1024 * 1024;
    final long fourMB = 4 * 1024 * 1024;

    @Test
    public void testSize() {
        final Row row = new Row.FakeRow();
        final int rowListSize = 1000;
        final List<Put> rowList = new ArrayList<>(rowListSize);
        for (int i = 0; i < rowListSize; i++) {
            rowList.add(row.put());
        }
        final double overheadMultiplier = 1.5;
        final long numberOfRows = rowList.size();
        final long averageSize = rowList.get(0).heapSize();
        final long estimatedBatchSize = Math.round(numberOfRows * (averageSize * overheadMultiplier));
        final BufferSizeFromRowList bufferSizeFromRowList = new BufferSizeFromRowList(
                rowList,
                overheadMultiplier,
                twoMB,
                fourMB
        );
        final long bufferSize = bufferSizeFromRowList.value();
        Assertions.assertNotEquals(twoMB, estimatedBatchSize);
        Assertions.assertNotEquals(fourMB, estimatedBatchSize);
        Assertions.assertEquals(estimatedBatchSize, bufferSize);
    }

    @Test
    public void testMinSize() {
        final Row row = new Row.FakeRow();
        final int rowListSize = 10;
        final List<Put> rowList = new ArrayList<>(rowListSize);
        for (int i = 0; i < rowListSize; i++) {
            rowList.add(row.put());
        }
        final BufferSizeFromRowList bufferSizeFromRowList = new BufferSizeFromRowList(rowList, 1.0, twoMB, fourMB);
        final long bufferSize = bufferSizeFromRowList.value();
        Assertions.assertEquals(twoMB, bufferSize, "minimum size should be 2MB");
    }

    @Test
    public void testMaxSize() {
        final Row row = new Row.FakeRow();
        final int rowListSize = 10000;
        final List<Put> rowList = new ArrayList<>(rowListSize);
        for (int i = 0; i < rowListSize; i++) {
            rowList.add(row.put());
        }
        final double overheadMultiplier = 5.0;
        final long numberOfRows = rowList.size();
        final long averageSize = rowList.get(0).heapSize();
        final long estimatedBatchSize = Math.round(numberOfRows * (averageSize * overheadMultiplier));
        final BufferSizeFromRowList bufferSizeFromRowList = new BufferSizeFromRowList(rowList, overheadMultiplier);
        final long bufferSize = bufferSizeFromRowList.value();
        Assertions.assertTrue(estimatedBatchSize > 64 * 1024 * 1024);
        Assertions.assertEquals(64 * 1024 * 1024, bufferSize, "max size should be 64MB");
    }

    @Test
    public void testMinSizeOnEmptyList() {
        final List<Put> rowList = new ArrayList<>();
        final double overheadMultiplier = 2.0;
        final BufferSizeFromRowList bufferSizeFromRowList = new BufferSizeFromRowList(
                rowList,
                overheadMultiplier,
                twoMB,
                fourMB
        );
        final long bufferSize = bufferSizeFromRowList.value();
        Assertions.assertEquals(twoMB, bufferSize);
    }
}
