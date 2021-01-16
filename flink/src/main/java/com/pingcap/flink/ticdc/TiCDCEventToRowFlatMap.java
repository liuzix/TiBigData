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

import com.google.common.collect.ImmutableMap;
import com.pingcap.flink.ticdc.schema.TiCDCEvent;
import com.pingcap.flink.ticdc.schema.TiCDCEventColumn;
import com.pingcap.flink.ticdc.schema.TiCDCEventRowChange;
import com.pingcap.flink.ticdc.schema.TiCDCEventType;
import com.zhihu.tibigdata.flink.tidb.TypeUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.tikv.common.meta.TiColumnInfo;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class TiCDCEventToRowFlatMap implements FlatMapFunction<TiCDCEvent, RowData> {
    private final Map<String, Integer> nameToPosMap;

    private final RowTypeInfo rowTypeInfo;

    public TiCDCEventToRowFlatMap(Map<String, String> properties, String tableName) {
        List<TiColumnInfo> columnInfos = TiCDCUtils.getTiDBSchema(properties, tableName);
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        TypeInformation<?>[] typeInfos = new TypeInformation<?>[columnInfos.size() + 1];
        String[] names = new String[columnInfos.size() + 1];
        for (int i = 0; i < columnInfos.size(); i++) {
            builder.put(columnInfos.get(i).getName(), i);
            DataType flinkType = TypeUtils.getFlinkType(columnInfos.get(i).getType());
            typeInfos[i] = TypeInformation.of(flinkType.getConversionClass());
            names[i] = columnInfos.get(i).getName();
        }

        builder.put("rowtime", columnInfos.size());
        typeInfos[columnInfos.size()] = TypeInformation.of(Timestamp.class);
        names[columnInfos.size()] = "rowtime";

        this.nameToPosMap = builder.build();

        this.rowTypeInfo = new RowTypeInfo(typeInfos, names);
    }

    @Override
    public void flatMap(TiCDCEvent value, Collector<RowData> out) throws Exception {
        if (value.getTiCDCEventValue().getType() != TiCDCEventType.rowChange) {
            return;
        }

        RowKind rowKind;
        TiCDCEventRowChange rowChangeEvent = (TiCDCEventRowChange)value.getTiCDCEventValue();
        if (rowChangeEvent.getUpdateOrDelete().equals("d")) {
            rowKind = RowKind.DELETE;
        } else if (rowChangeEvent.getOldColumns() != null) {
            rowKind = RowKind.UPDATE_AFTER;
        } else {
            rowKind = RowKind.INSERT;
        }

        int rowSize = rowChangeEvent.getColumns().size();
        GenericRowData row = new GenericRowData(rowKind, rowSize + 1);
        for (int i = 0; i < rowSize; i++) {
            TiCDCEventColumn column = rowChangeEvent.getColumns().get(i);
            int pos = this.nameToPosMap.get(column.getName());
            row.setField(pos, column.getV());
        }

        row.setField(rowSize, new Timestamp(value.getTiCDCEventKey().getTs()));
        out.collect(row);
    }

    public RowTypeInfo getTypeInfo() {
        return this.rowTypeInfo;
    }
}
