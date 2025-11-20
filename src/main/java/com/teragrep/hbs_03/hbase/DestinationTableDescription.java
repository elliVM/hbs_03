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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Description of the HBase replication destination table
 */
public final class DestinationTableDescription implements TableDescription {

    private final TableName name;
    private final boolean useCompression;
    private final Compression.Algorithm compressionAlgorithm;

    public DestinationTableDescription(final String tableName) {
        this(TableName.valueOf(tableName));
    }

    public DestinationTableDescription(final TableName tableName) {
        this(tableName, false, Compression.Algorithm.NONE);
    }

    public DestinationTableDescription(final String name, final boolean useCompression) {
        this(TableName.valueOf(name), useCompression);
    }

    public DestinationTableDescription(final TableName tableName, final boolean useCompression) {
        this(tableName, useCompression, Compression.Algorithm.SNAPPY);
    }

    public DestinationTableDescription(final String name, final Compression.Algorithm compressionAlgorithm) {
        this(TableName.valueOf(name), compressionAlgorithm);
    }

    public DestinationTableDescription(final TableName tableName, final Compression.Algorithm compressionAlgorithm) {
        this(tableName, true, compressionAlgorithm);
    }

    public DestinationTableDescription(
            final TableName name,
            final boolean useCompression,
            final Compression.Algorithm compressionAlgorithm
    ) {
        this.name = name;
        this.useCompression = useCompression;
        this.compressionAlgorithm = compressionAlgorithm;
    }

    @Override
    public TableName name() {
        return name;
    }

    @Override
    public TableDescriptor describe() {
        return TableDescriptorBuilder
                .newBuilder(name)
                .setColumnFamilies(columnFamilyDescriptions())
                .setReadOnly(false)
                .build();
    }

    private List<ColumnFamilyDescriptor> columnFamilyDescriptions() {
        final ColumnFamilyDescriptorBuilder metaFamilyBuilder = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("meta"))
                .setMaxVersions(1) // number of allowed copies per column e.g., with the same row key
                .setBloomFilterType(BloomType.ROW);

        final ColumnFamilyDescriptorBuilder bloomFamilyBuilder = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("bloom"))
                .setMaxVersions(1) // number of allowed copies per column e.g., with the same row key
                .setBloomFilterType(BloomType.ROW)
                .setBlockCacheEnabled(false); // do not keep bloom filter blocks in cache memory

        if (useCompression) {
            metaFamilyBuilder.setCompressionType(compressionAlgorithm);
            bloomFamilyBuilder.setCompressionType(compressionAlgorithm);
        }

        final ColumnFamilyDescriptor metaFamilyDescriptor = metaFamilyBuilder.build();
        final ColumnFamilyDescriptor bloomFamilyDescriptor = bloomFamilyBuilder.build();

        return Collections.unmodifiableList(Arrays.asList(metaFamilyDescriptor, bloomFamilyDescriptor));
    }
}
