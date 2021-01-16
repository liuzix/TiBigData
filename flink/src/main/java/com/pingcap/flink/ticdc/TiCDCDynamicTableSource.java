package com.pingcap.flink.ticdc;

import com.zhihu.tibigdata.flink.tidb.TiDBDynamicTableFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.Map;

public class TiCDCDynamicTableSource implements ScanTableSource {
    private final Map<String, String> properties;

    private final TableSchema tableSchema;

    public TiCDCDynamicTableSource(Map<String, String> properties, TableSchema tableSchema) {
        this.properties = properties;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        TypeInformation<RowData> typeInformation = runtimeProviderContext.createTypeInformation(tableSchema.toRowDataType());
        String databaseName = properties.get(TiDBDynamicTableFactory.DATABASE_NAME.key());
        String tableName = properties.get(TiDBDynamicTableFactory.TABLE_NAME.key());
        return new TiCDCDataStreamScanProvider(this.properties, databaseName + "." + tableName, typeInformation);
    }

    @Override
    public DynamicTableSource copy() {
        return new TiCDCDynamicTableSource(this.properties, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return this.getClass().getName();
    }
}
