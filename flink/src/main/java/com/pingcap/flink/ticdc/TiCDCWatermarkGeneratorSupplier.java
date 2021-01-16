package com.pingcap.flink.ticdc;

import com.pingcap.flink.ticdc.schema.TiCDCEvent;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;

public class TiCDCWatermarkGeneratorSupplier implements WatermarkGeneratorSupplier<TiCDCEvent>, TimestampAssignerSupplier<TiCDCEvent> {
    @Override
    public TimestampAssigner<TiCDCEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TiCDCWatermarkGenerator();
    }

    @Override
    public WatermarkGenerator<TiCDCEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new TiCDCWatermarkGenerator();
    }
}
