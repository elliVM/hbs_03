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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public final class MutatorParamsSourceFromListTest {

    @Test
    public void testTableName() {
        final Row row = new Row.FakeRow();
        final List<Put> puts = List.of(row.put());
        final TableName name = TableName.valueOf("test");
        final BufferedMutatorParams params = new MutatorParamsSourceFromList(name, puts).value();
        Assertions.assertEquals(name, params.getTableName());
    }

    @Test
    public void testMinimumSize() {
        final Row row = new Row.FakeRow();
        final List<Put> puts = List.of(row.put());
        final long minimumSize = 2 * 1024 * 1024; // 2MB
        final long bufferSize = new MutatorParamsSourceFromList(TableName.valueOf("test"), puts, 2.0)
                .value()
                .getWriteBufferSize();
        Assertions.assertEquals(minimumSize, bufferSize);
    }

    @Test
    public void testDynamicSize() {
        final Row row = new Row.FakeRow();
        final int rowListSize = 1000;
        final double overheadMultiplier = 3.0;
        final List<Put> rowList = new ArrayList<>(rowListSize);
        for (int i = 0; i < rowListSize; i++) {
            rowList.add(row.put());
        }
        final long estimatedBufferSize = Math.round(rowListSize * (row.put().heapSize() * overheadMultiplier));
        final long bufferSize = new MutatorParamsSourceFromList(TableName.valueOf("test"), rowList, overheadMultiplier)
                .value()
                .getWriteBufferSize();
        Assertions.assertEquals(estimatedBufferSize, bufferSize);
    }
}
