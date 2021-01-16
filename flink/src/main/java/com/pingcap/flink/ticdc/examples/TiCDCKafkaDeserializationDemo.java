package com.pingcap.flink.ticdc.examples;

import com.pingcap.flink.ticdc.TiCDCKafkaDeserializationSchema;
import com.pingcap.flink.ticdc.TiCDCWatermarkGeneratorSupplier;
import com.pingcap.flink.ticdc.schema.TiCDCEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;

public class TiCDCKafkaDeserializationDemo {
    public static void main(String[] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String kafkaServers = parameterTool.getRequired("tidb.cdc.kafka.bootstrap.servers");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaServers);
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumerBase<TiCDCEvent> consumer =
                new FlinkKafkaConsumer<>("test", new TiCDCKafkaDeserializationSchema(), properties)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new TiCDCWatermarkGeneratorSupplier()).withTimestampAssigner(new TiCDCWatermarkGeneratorSupplier()));

        DataStream<TiCDCEvent> stream = env
                .addSource(consumer);

        stream.print();
        env.execute();
    }
}
