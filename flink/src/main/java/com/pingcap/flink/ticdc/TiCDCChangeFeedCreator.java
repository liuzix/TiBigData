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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TiCDCChangeFeedCreator {
    static final Logger LOG = LoggerFactory.getLogger(TiCDCChangeFeedCreator.class);

    private final Map<String, String> properties;

    private final String cdcBinPath;

    private final String pdServer;

    private final String kafkaServers;

    public TiCDCChangeFeedCreator(Map<String, String> properties) {
        this.properties = Preconditions.checkNotNull(properties);
        this.cdcBinPath = this.properties.get("tidb.cdc.bin.path");
        this.pdServer = this.properties.get("tidb.pd.servers");
        this.kafkaServers = this.properties.get("tidb.cdc.kafka.bootstrap.servers");
        LOG.debug("Using cdcBinPath = " + cdcBinPath
                + ", kafkaServers = " + kafkaServers
                + ", pdServer = " + pdServer);
    }

    public String createChangeFeed(String tableName, TiTimestamp startTs) throws IOException, InterruptedException {
        String changeFeedName = tableName.replace('.', '-') + "-" + startTs.getVersion();
        if (this.checkChangeFeedExists(changeFeedName)) {
            return changeFeedName;
        }

        String configFileName = "/tmp/ticdc-" + changeFeedName + ".toml";
        FileWriter configFile = new FileWriter(configFileName);
        configFile.append("[filter]\nrules = ['").append(tableName).append("']\n");
        configFile.flush();
        configFile.close();

        String kafkaTopic = changeFeedName.replace('-', '_');

        String sinkURI = "kafka://" + this.kafkaServers + "/" + kafkaTopic;
        ProcessBuilder processBuilder = new ProcessBuilder(
                this.cdcBinPath,
                "cli",
                "--pd=http://" + pdServer,
                "changefeed",
                "create",
                "-c",
               changeFeedName,
                "--sink-uri",
                sinkURI,
                "--config",
                configFileName
                );

        Process process = processBuilder.start();
        process.waitFor(10, TimeUnit.SECONDS);
        String outStr = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
        if (process.exitValue() != 0) {
            LOG.warn("CDC cli exited with error: " + outStr);
        }

        if (outStr.contains("Create changefeed successfully")) {
            return kafkaTopic;
        }

        LOG.warn("CDC cli exited with unexpected result: " + outStr);
        return "";
    }

    public boolean checkChangeFeedExists(String cfID) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(
                this.cdcBinPath,
                "cli",
                "--pd=http://" + pdServer,
                "changefeed",
                "list");
        Process process = processBuilder.start();
        process.waitFor(10, TimeUnit.SECONDS);
        String outStr = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
        if (process.exitValue() != 0) {
            LOG.warn("CDC cli exited with error: " + outStr);
        }

        JSONArray changeFeeds = JSON.parseArray(outStr);
        for (int i = 0; i < changeFeeds.size(); i++) {
            JSONObject jsonObject = changeFeeds.getJSONObject(i);
            String id = jsonObject.getString("id");
            if (id.equals(cfID)) {
                return true;
            }
        }

        return false;
    }
}
