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
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class DestinationTableDescription {

    private final TableName name;

    public DestinationTableDescription(final String tableName) {
        this(TableName.valueOf(tableName));
    }

    public DestinationTableDescription(final TableName name) {
        this.name = name;
    }

    public TableDescriptor description() {
        return TableDescriptorBuilder
                .newBuilder(name)
                .setColumnFamilies(columnFamilyDescriptions())
                .setReadOnly(false)
                .build();
    }

    private List<ColumnFamilyDescriptor> columnFamilyDescriptions() {
        final ColumnFamilyDescriptor metaFamilyBuilder = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("meta"))
                .setMaxVersions(1) // number of allowed copies per column e.g. with the same row key
                .setBloomFilterType(BloomType.ROW)
                .build();

        final ColumnFamilyDescriptor bloomFamilyBuilder = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("bloom"))
                .setMaxVersions(1) // number of allowed copies per column e.g. with the same row key
                .setBloomFilterType(BloomType.ROW)
                .build();

        return Collections.unmodifiableList(Arrays.asList(metaFamilyBuilder, bloomFamilyBuilder));
    }
}
