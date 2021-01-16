package com.pingcap.flink.ticdc;

import com.zhihu.tibigdata.flink.tidb.TiDBDynamicTableFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.dialect.MySQLDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarks;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.runtime.conversion.CRowToRowMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.Map;

import static com.zhihu.tibigdata.jdbc.TiDBDriver.MYSQL_DRIVER_NAME;
import static com.zhihu.tibigdata.jdbc.TiDBDriver.TIDB_PREFIX;
import static com.zhihu.tibigdata.tidb.ClientConfig.TIDB_DRIVER_NAME;

public class TiCDCDataStreamScanProvider implements DataStreamScanProvider {
    static final Logger LOG = LoggerFactory.getLogger(TiCDCDataStreamScanProvider.class);

    private final Map<String, String> properties;

    private final String tableName;

    private final TypeInformation<RowData> typeInformation;

    private final TableSchema tableSchema;

    public TiCDCDataStreamScanProvider(
            Map<String, String> properties,
            String tableName,
            TypeInformation<RowData> typeInformation,
            TableSchema tableSchema) {
        this.properties = Preconditions.checkNotNull(properties);
        this.tableName = tableName;
        this.typeInformation = typeInformation;
        this.tableSchema = tableSchema;
    }

    @Override
    public DataStream<RowData> produceDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        TiCDCStreamProvider streamProvider = new TiCDCStreamProvider(this.properties, streamExecutionEnvironment);
        DataStream<RowData> incrementalStream;
        try {
            TiTimestamp startTs = TiCDCUtils.getCurrentTs(properties);
            LOG.info("produceDataStream: starting from {}", startTs.getVersion());
            TiCDCEventToRowFlatMap flatMap = new TiCDCEventToRowFlatMap(properties, tableName);
            incrementalStream = streamProvider.CreateIncrementalStream(this.tableName, startTs).flatMap(flatMap).returns(typeInformation);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);
        Table table = tableEnvironment.fromTableSource(this.makeJDBCSource());
        DataStream<RowData> historicalStream = tableEnvironment.toAppendStream(table, RowData.class);

        return historicalStream.union(incrementalStream);
    }

    @Override
    public boolean isBounded() {
        return false;
    }

    private JdbcTableSource makeJDBCSource() {
        String dbUrl = this.properties.get("tidb.database.url");
        String databaseName = properties.get(TiDBDynamicTableFactory.DATABASE_NAME.key());
        String tableName = properties.get(TiDBDynamicTableFactory.TABLE_NAME.key());
        // jdbc options
        dbUrl = dbUrl.substring(0, dbUrl.lastIndexOf("/") + 1) + databaseName;
        String driverName = dbUrl.startsWith(TIDB_PREFIX) ? TIDB_DRIVER_NAME : MYSQL_DRIVER_NAME;
        JdbcOptions jdbcOptions = JdbcOptions.builder()
                .setDBUrl(dbUrl)
                .setTableName(tableName)
                .setUsername(properties.getOrDefault("tidb.username", "root"))
                .setPassword(properties.getOrDefault("tidb.password", ""))
                .setDialect(new MySQLDialect())
                .setDriverName(driverName)
                .build();


        JdbcTableSource.Builder jdbcTableSourceBuilder = JdbcTableSource.builder();
        return jdbcTableSourceBuilder.setOptions(jdbcOptions).setSchema(tableSchema).build();
    }
}
