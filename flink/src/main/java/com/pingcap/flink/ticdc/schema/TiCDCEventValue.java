/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.flink.ticdc.schema;

import com.pingcap.flink.ticdc.KafkaMessage;

public class TiCDCEventValue {
    private TiCDCEventType type;
    private int kafkaPartition;
    private long kafkaOffset;
    private long kafkaTimestamp;

    public TiCDCEventValue() {
    }

    public TiCDCEventValue(TiCDCEventType type, KafkaMessage kafkaMessage) {
        this.type = type;
        this.kafkaPartition = kafkaMessage.getPartition();
        this.kafkaOffset = kafkaMessage.getOffset();
        this.kafkaTimestamp = kafkaMessage.getTimestamp();
    }

    public TiCDCEventType getType() {
        return type;
    }

    public void setType(TiCDCEventType type) {
        this.type = type;
    }

    public int getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(int kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public long getKafkaOffset() {
        return kafkaOffset;
    }

    public void setKafkaOffset(long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

    public long getKafkaTimestamp() {
        return kafkaTimestamp;
    }

    public void setKafkaTimestamp(long kafkaTimestamp) {
        this.kafkaTimestamp = kafkaTimestamp;
    }
}