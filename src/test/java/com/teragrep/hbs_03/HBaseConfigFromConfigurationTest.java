package com.teragrep.hbs_03;

import com.teragrep.cnf_01.Configuration;
import com.teragrep.cnf_01.PropertiesConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class HBaseConfigFromConfigurationTest {

    @Test
    public void testDefaultConfig() {
        final Properties props = new Properties();
        final Configuration configuration = new PropertiesConfiguration(props);
        final org.apache.hadoop.conf.Configuration hbaseConfig = new HBaseConfigFromConfiguration(configuration, "prefix.").config();
        Assertions.assertEquals("localhost", hbaseConfig.get("hbase.zookeeper.quorum"));
    }

    @Test
    public void testConfig() {
        final Properties props = new Properties();
        props.put("prefix.zookeeper.quorum", "testhost");
        final Configuration configuration = new PropertiesConfiguration(props);
        final org.apache.hadoop.conf.Configuration hbaseConfig = new HBaseConfigFromConfiguration(configuration, "prefix.").config();
        Assertions.assertEquals("testhost", hbaseConfig.get("zookeeper.quorum"));

    }

    @Test
    public void testConfigFromFile() {
        final Properties props = new Properties();
        props.put("prefix.config.path", "src/test/resources/hbase-site.xml");
        final Configuration configuration = new PropertiesConfiguration(props);
        final org.apache.hadoop.conf.Configuration hbaseConfig = new HBaseConfigFromConfiguration(configuration, "prefix.").config();
        Assertions.assertEquals("host_from_file", hbaseConfig.get("hbase.zookeeper.quorum"));
    }

    @Test
    public void testInvalidFilePath() {
        final Properties props = new Properties();
        props.put("prefix.config.path", "broken/path/");
        final Configuration configuration = new PropertiesConfiguration(props);
        org.apache.hadoop.conf.Configuration hbaseConfig = Assertions.assertDoesNotThrow(() -> new HBaseConfigFromConfiguration(configuration, "prefix.").config());
        Assertions.assertEquals("localhost", hbaseConfig.get("hbase.zookeeper.quorum"));
    }
}