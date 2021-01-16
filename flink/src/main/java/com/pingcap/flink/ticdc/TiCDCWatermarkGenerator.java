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
import com.pingcap.flink.ticdc.schema.TiCDCEventType;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class TiCDCWatermarkGenerator implements WatermarkGenerator<TiCDCEvent>, TimestampAssigner<TiCDCEvent> {
    private long resolvedTs = 0;

    @Override
    public void onEvent(TiCDCEvent event, long eventTimestamp, WatermarkOutput output) {
        if (event.getTiCDCEventValue().getType() == TiCDCEventType.resolved) {
            resolvedTs = event.getTiCDCEventKey().getTs();
            output.emitWatermark(new Watermark(resolvedTs));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(resolvedTs));
    }

    @Override
    public long extractTimestamp(TiCDCEvent element, long recordTimestamp) {
        return element.getTiCDCEventKey().getTs();
    }
}
