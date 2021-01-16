/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.flink.ticdc.examples;

import com.pingcap.flink.ticdc.TiCDCEventToRowFlatMap;
import com.pingcap.flink.ticdc.TiCDCStreamProvider;
import com.pingcap.flink.ticdc.TiCDCUtils;
import com.pingcap.flink.ticdc.schema.TiCDCEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.tikv.common.meta.TiTimestamp;

import java.util.HashMap;
import java.util.Map;

public class TiCDCStreamTableDemo {
    public static void main(String[] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("tidb.cdc.bin.path", "/home/zixiong/ticdc/bin/cdc");
        properties.put("tidb.pd.servers", "127.0.0.1:2379");
        properties.put("tidb.cdc.kafka.bootstrap.servers", "127.0.0.1:9092");
        properties.put("tidb.database.url", "jdbc:mysql://address=(protocol=tcp)(host=127.0.0.1)(port=4000)");
        properties.put("tidb.username", "root");

        final TiCDCStreamProvider streamProvider = new TiCDCStreamProvider(properties, env);
        TiCDCEventToRowFlatMap flatMap = new TiCDCEventToRowFlatMap(properties, "testdb.test");
        TiTimestamp startTs = TiCDCUtils.getCurrentTs(properties);
        DataStream<Row> stream = streamProvider.CreateIncrementalStream("testdb.test", startTs).flatMap(flatMap).returns(flatMap.getTypeInfo());
        tableEnvironment.createTemporaryView("test", stream);

        tableEnvironment.sqlQuery("select * from test").execute().print();
        env.execute();
    }
}
