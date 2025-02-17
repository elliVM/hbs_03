package com.teragrep.hbs_03;

import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DynamicMutatorParamsTest {

    @Test
    public void testDynamicBufferSize() {
        DynamicMutatorParams dynamicMutatorParams = new DynamicMutatorParams("test", 5, 500);
        BufferedMutatorParams params = dynamicMutatorParams.params();
        long bufferSize = params.getWriteBufferSize();
        Assert.assertEquals(2500, bufferSize); // 5 * 500
    }

    @Test
    public void testMaxBufferSize() {
        DynamicMutatorParams dynamicMutatorParams = new DynamicMutatorParams("test", 1000, 10000000);
        BufferedMutatorParams params = dynamicMutatorParams.params();
        long bufferSize = params.getWriteBufferSize();
        Assert.assertEquals(67108864, bufferSize); // 64MB
    }

}