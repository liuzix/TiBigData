package com.pingcap.flink.ticdc;

import com.pingcap.flink.ticdc.schema.TiCDCEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.tikv.common.meta.TiTimestamp;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TiCDCStreamProviderTest {
    @Test
    void createIncrementalStream() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("tidb.cdc.bin.path", "/home/zixiong/ticdc/bin/cdc");
        properties.put("tidb.pd.servers", "127.0.0.1:2379");
        properties.put("tidb.cdc.kafka.bootstrap.servers", "127.0.0.1:9092");
        properties.put("tidb.database.url", "jdbc:mysql://address=(protocol=tcp)(host=127.0.0.1)(port=4000)");
        properties.put("tidb.username", "root");

        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final TiCDCStreamProvider streamProvider = new TiCDCStreamProvider(properties, env);

        try {
            TiTimestamp startTs = TiCDCUtils.getCurrentTs(properties);
            DataStream<TiCDCEvent> stream = streamProvider.CreateIncrementalStream("testdb.test", startTs);
            Assertions.assertNotNull(stream);
            stream.print();
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }
}