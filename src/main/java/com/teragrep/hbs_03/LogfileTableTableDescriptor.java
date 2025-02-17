package com.teragrep.hbs_03;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LogfileTableTableDescriptor {
    private final TableName name;

    public LogfileTableTableDescriptor(final String tableName) {
        this(TableName.valueOf(tableName));
    }
    public LogfileTableTableDescriptor(final TableName name) {
        this.name = name;
    }

    public TableDescriptor descriptor() {
        return TableDescriptorBuilder
                .newBuilder(name)
                .setColumnFamilies(columnFamilyDescriptors())
                //.setDurability(Durability.FSYNC_WAL) // highest level of durability
                //.setRegionReplication(3)
                .setReadOnly(false)
                .build();
    }

    public TableName name() {
        return name;
    }

    private List<ColumnFamilyDescriptor> columnFamilyDescriptors() {
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
