package com.pingcap.flink.ticdc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;
import org.w3c.dom.TypeInfo;

import java.util.Map;

public class TiCDCDataStreamScanProvider implements DataStreamScanProvider {
    static final Logger LOG = LoggerFactory.getLogger(TiCDCDataStreamScanProvider.class);

    private final Map<String, String> properties;

    private final String tableName;

    private final TypeInformation<RowData> typeInformation;

    public TiCDCDataStreamScanProvider(Map<String, String> properties, String tableName, TypeInformation<RowData> typeInformation) {
        this.properties = Preconditions.checkNotNull(properties);
        this.tableName = tableName;
        this.typeInformation = typeInformation;
    }

    @Override
    public DataStream<RowData> produceDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        TiCDCStreamProvider streamProvider = new TiCDCStreamProvider(this.properties, streamExecutionEnvironment);
        try {
            TiTimestamp startTs = TiCDCUtils.getCurrentTs(properties);
            LOG.info("produceDataStream: starting from {}", startTs.getVersion());
            TiCDCEventToRowFlatMap flatMap = new TiCDCEventToRowFlatMap(properties, tableName);
            return streamProvider.CreateIncrementalStream(this.tableName, startTs).flatMap(flatMap).returns(typeInformation);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isBounded() {
        return false;
    }
}
