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
