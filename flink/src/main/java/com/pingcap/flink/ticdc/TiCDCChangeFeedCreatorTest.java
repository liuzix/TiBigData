package com.pingcap.flink.ticdc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.tikv.common.meta.TiTimestamp;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

class TiCDCChangeFeedCreatorTest {
    private static TiCDCChangeFeedCreator changeFeedCreator = null;

    @BeforeAll
    static void setUp() {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.DEBUG);

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("tidb.cdc.bin.path", "/home/zixiong/ticdc/bin/cdc");
        properties.put("tidb.pd.servers", "127.0.0.1:2379");
        properties.put("tidb.cdc.kafka.bootstrap.servers", "127.0.0.1:9092");
        changeFeedCreator = new TiCDCChangeFeedCreator(properties);
    }

    @Test
    void checkChangeFeedExists() {
        try {
            boolean doesExist = changeFeedCreator.checkChangeFeedExists("test-kafka");
            Assertions.assertTrue(doesExist);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    @Test
    void createChangeFeed() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("tidb.database.url", "jdbc:mysql://address=(protocol=tcp)(host=127.0.0.1)(port=4000)");
        properties.put("tidb.username", "root");

        try {
            Random rand = new Random();
            int randSuffix = rand.nextInt(128);
            TiTimestamp startTs = TiCDCUtils.getCurrentTs(properties);
            String topic = changeFeedCreator.createChangeFeed("testdb.test" + randSuffix, startTs);
            Assertions.assertEquals(topic, "testdb_test" + randSuffix + "_" + startTs.getVersion());
            boolean doesExist = changeFeedCreator.checkChangeFeedExists("testdb-test" + randSuffix + "-" + startTs.getVersion());
            Assertions.assertTrue(doesExist);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }
}