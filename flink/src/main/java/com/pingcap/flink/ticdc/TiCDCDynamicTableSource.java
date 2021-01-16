package com.pingcap.flink.ticdc;

import com.google.common.collect.ImmutableSet;
import com.zhihu.tibigdata.flink.tidb.TiDBDynamicTableFactory;
import com.zhihu.tibigdata.tidb.ClientConfig;
import com.zhihu.tibigdata.tidb.ClientSession;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.dialect.MySQLDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcLookupFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Map;
import java.util.Set;

import static com.zhihu.tibigdata.jdbc.TiDBDriver.MYSQL_DRIVER_NAME;
import static com.zhihu.tibigdata.jdbc.TiDBDriver.TIDB_PREFIX;
import static com.zhihu.tibigdata.tidb.ClientConfig.TIDB_DRIVER_NAME;

public class TiCDCDynamicTableSource implements ScanTableSource, LookupTableSource {
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
        return new TiCDCDataStreamScanProvider(
                this.properties,
                databaseName + "." + tableName,
                typeInformation,
                this.tableSchema);
    }

    @Override
    public DynamicTableSource copy() {
        return new TiCDCDynamicTableSource(this.properties, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return this.getClass().getName();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        JdbcLookupFunction.Builder jdbcBuilder = JdbcLookupFunction.builder();
        String dbUrl = this.properties.get("tidb.database.url");

        String databaseName = properties.get(TiDBDynamicTableFactory.DATABASE_NAME.key());
        String tableName = properties.get(TiDBDynamicTableFactory.TABLE_NAME.key());

        dbUrl = dbUrl.substring(0, dbUrl.lastIndexOf("/") + 1) + databaseName;
        String driverName = dbUrl.startsWith(TIDB_PREFIX) ? TIDB_DRIVER_NAME : MYSQL_DRIVER_NAME;

        // jdbc options
        JdbcOptions jdbcOptions = JdbcOptions.builder()
                .setDBUrl(dbUrl)
                .setTableName(tableName)
                .setUsername(properties.getOrDefault("tidb.username", "root"))
                .setPassword(properties.getOrDefault("tidb.password", ""))
                .setDialect(new MySQLDialect())
                .setDriverName(driverName)
                .build();

        // get keys
        String[] keyFields;
        try (ClientSession clientSession = ClientSession.createWithSingleConnection(
                new ClientConfig(this.properties))) {
            Set<String> set = ImmutableSet.<String>builder()
                    .addAll(clientSession.getUniqueKeyColumns(databaseName, tableName))
                    .addAll(clientSession.getPrimaryKeyColumns(databaseName, tableName))
                    .build();
            keyFields = set.size() == 0 ? null : set.toArray(new String[0]);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        JdbcLookupOptions jdbcLookupOptions = JdbcLookupOptions.builder().setCacheExpireMs(-1).build();

        TypeInformation<?>[] typeInformations = new TypeInformation<?>[this.tableSchema.getFieldCount()];
        for (int i = 0; i < this.tableSchema.getFieldCount(); i++) {
            DataType dataType = this.tableSchema.getFieldDataType(i).get();
            typeInformations[i] = context.createTypeInformation(dataType);
        }

        JdbcLookupFunction jdbcLookupFunction = jdbcBuilder
                .setOptions(jdbcOptions)
                .setLookupOptions(jdbcLookupOptions)
                .setFieldNames(this.tableSchema.getFieldNames())
                .setFieldTypes(typeInformations)
                .setKeyNames(keyFields)
                .build();

        return TableFunctionProvider.of(jdbcLookupFunction);
    }
}
