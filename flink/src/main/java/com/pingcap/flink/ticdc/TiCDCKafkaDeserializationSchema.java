package com.pingcap.flink.ticdc;

import com.pingcap.flink.ticdc.schema.TiCDCEvent;
import com.pingcap.flink.ticdc.schema.TiCDCEventDecoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TiCDCKafkaDeserializationSchema implements KafkaDeserializationSchema<TiCDCEvent> {
    @Override
    public boolean isEndOfStream(TiCDCEvent tiCDCEvent) {
        return false;
    }

    @Override
    public TiCDCEvent deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return null;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<TiCDCEvent> out) throws Exception {
        KafkaMessage kafkaMessage = new KafkaMessage(message.key(), message.value());
        kafkaMessage.setOffset(message.offset());
        kafkaMessage.setPartition(message.partition());
        kafkaMessage.setTimestamp(message.timestamp());

        TiCDCEventDecoder decoder = new TiCDCEventDecoder(kafkaMessage);
        while (decoder.hasNext()) {
            TiCDCEvent event = decoder.next();
            out.collect(event);
        }
    }

    @Override
    public TypeInformation<TiCDCEvent> getProducedType() {
        return TypeInformation.of(TiCDCEvent.class);
    }
}
