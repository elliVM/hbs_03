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
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class DestinationTableDescriptionTest {

    @Test
    public void testDescription() {
        final DestinationTableDescription destinationTableDescription = new DestinationTableDescription("test_target");
        final TableName name = destinationTableDescription.name();
        final TableDescriptor descriptor = destinationTableDescription.describe();
        Assertions.assertEquals("test_target", name.getNameAsString());
        Assertions.assertEquals(2, descriptor.getColumnFamilyCount());
        final ColumnFamilyDescriptor meta = descriptor.getColumnFamily(Bytes.toBytes("meta"));
        final ColumnFamilyDescriptor bloom = descriptor.getColumnFamily(Bytes.toBytes("bloom"));
        Assertions.assertEquals(1, meta.getMaxVersions());
        Assertions.assertEquals(BloomType.ROW, meta.getBloomFilterType());
        Assertions.assertEquals(Compression.Algorithm.NONE, meta.getCompactionCompressionType());
        Assertions.assertTrue(meta.isBlockCacheEnabled());
        Assertions.assertEquals(1, bloom.getMaxVersions());
        Assertions.assertEquals(BloomType.ROW, bloom.getBloomFilterType());
        Assertions.assertEquals(Compression.Algorithm.NONE, bloom.getCompactionCompressionType());
        Assertions.assertFalse(bloom.isBlockCacheEnabled());
    }

    @Test
    public void testCompressionEnableOption() {
        final DestinationTableDescription destinationTableDescription = new DestinationTableDescription(
                "test_target",
                true
        );
        final TableName name = destinationTableDescription.name();
        final TableDescriptor descriptor = destinationTableDescription.describe();
        Assertions.assertEquals("test_target", name.getNameAsString());
        Assertions.assertEquals(2, descriptor.getColumnFamilyCount());
        final ColumnFamilyDescriptor meta = descriptor.getColumnFamily(Bytes.toBytes("meta"));
        final ColumnFamilyDescriptor bloom = descriptor.getColumnFamily(Bytes.toBytes("bloom"));
        Assertions.assertEquals(Compression.Algorithm.SNAPPY, meta.getCompactionCompressionType());
        Assertions.assertEquals(Compression.Algorithm.SNAPPY, bloom.getCompactionCompressionType());
    }

    @Test
    public void testCompressionAlgorithmOption() {
        final DestinationTableDescription destinationTableDescription = new DestinationTableDescription(
                "test_target",
                Compression.Algorithm.LZ4
        );
        final TableName name = destinationTableDescription.name();
        final TableDescriptor descriptor = destinationTableDescription.describe();
        Assertions.assertEquals("test_target", name.getNameAsString());
        Assertions.assertEquals(2, descriptor.getColumnFamilyCount());
        final ColumnFamilyDescriptor meta = descriptor.getColumnFamily(Bytes.toBytes("meta"));
        final ColumnFamilyDescriptor bloom = descriptor.getColumnFamily(Bytes.toBytes("bloom"));
        Assertions.assertEquals(Compression.Algorithm.LZ4, meta.getCompactionCompressionType());
        Assertions.assertEquals(Compression.Algorithm.LZ4, bloom.getCompactionCompressionType());
    }
}
