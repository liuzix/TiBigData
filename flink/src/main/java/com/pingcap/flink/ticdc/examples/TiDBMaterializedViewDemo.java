package com.pingcap.flink.ticdc.examples;

import com.zhihu.tibigdata.flink.tidb.TiDBCatalog;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlannerConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class TiDBMaterializedViewDemo {
    public static void main(String[] args) {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final Map<String, String> properties = new HashMap<>(parameterTool.toMap());
        final String sql = parameterTool.getRequired("tidb.materialized.query");
        final String target = parameterTool.getRequired("tidb.materialized.target");
        properties.put("tidb.cdc.source.enabled", "true");
        properties.put("tidb.write_mode", "upsert");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
                .inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        // register TiDBCatalog
        TiDBCatalog catalog = new TiDBCatalog(properties);
        catalog.open();
        tableEnvironment.registerCatalog("tidb", catalog);

        tableEnvironment.useCatalog("tidb");
        tableEnvironment.useDatabase("testdb");
        String explanation = tableEnvironment.explainSql(sql);
        System.out.println(explanation);
        tableEnvironment.sqlQuery(sql).executeInsert(target);
    }
}
