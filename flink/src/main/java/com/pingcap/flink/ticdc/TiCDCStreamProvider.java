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

package com.pingcap.flink.ticdc;

import com.pingcap.flink.ticdc.schema.TiCDCEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

import java.util.Map;
import java.util.Properties;

public final class TiCDCStreamProvider {
    static final Logger LOG = LoggerFactory.getLogger(TiCDCStreamProvider.class);

    private final Map<String, String> properties;

    private final TiCDCChangeFeedCreator changeFeedCreator;

    private final StreamExecutionEnvironment env;

    public TiCDCStreamProvider(Map<String, String> properties, StreamExecutionEnvironment env) {
        this.properties = Preconditions.checkNotNull(properties);
        this.env = Preconditions.checkNotNull(env);
        this.changeFeedCreator = new TiCDCChangeFeedCreator(properties);
        LOG.debug("Stream provider initialized");
    }

    public DataStream<TiCDCEvent> CreateIncrementalStream(String tableName, TiTimestamp startTs) {
        LOG.info("CreateIncrementalStream: tableName = {}, startTs = {}", tableName, startTs.getVersion());
        try {
            String kafkaTopic = changeFeedCreator.createChangeFeed(tableName, startTs);
            LOG.info("CreateIncrementalStream: kafkaTopic = {}", kafkaTopic);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", this.properties.get("tidb.cdc.kafka.bootstrap.servers"));
            properties.setProperty("group.id", "flink");
            WatermarkStrategy<TiCDCEvent> watermarkStrategy =
                    WatermarkStrategy.forGenerator(new TiCDCWatermarkGeneratorSupplier()).withTimestampAssigner(new TiCDCWatermarkGeneratorSupplier());
            FlinkKafkaConsumerBase<TiCDCEvent> consumer =
                    new FlinkKafkaConsumer<>(kafkaTopic, new TiCDCKafkaDeserializationSchema(), properties).assignTimestampsAndWatermarks(watermarkStrategy);
            return env.addSource(consumer);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
