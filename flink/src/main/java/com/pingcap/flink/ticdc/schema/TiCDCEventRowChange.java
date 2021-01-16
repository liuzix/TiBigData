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

import java.util.List;

public class TiCDCEventRowChange extends TiCDCEventValue {
    private String updateOrDelete; // should be "u" or "d"
    private List<TiCDCEventColumn> oldColumns;
    private List<TiCDCEventColumn> columns;

    public TiCDCEventRowChange(KafkaMessage kafkaMessage) {
        super(TiCDCEventType.rowChange, kafkaMessage);
    }

    public String getUpdateOrDelete() {
        return updateOrDelete;
    }

    public void setUpdateOrDelete(String updateOrDelete) {
        this.updateOrDelete = updateOrDelete;
    }

    public List<TiCDCEventColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<TiCDCEventColumn> columns) {
        this.columns = columns;
    }

    public List<TiCDCEventColumn> getOldColumns() {
        return oldColumns;
    }

    public void setOldColumns(List<TiCDCEventColumn> oldColumns) {
        this.oldColumns = oldColumns;
    }
}