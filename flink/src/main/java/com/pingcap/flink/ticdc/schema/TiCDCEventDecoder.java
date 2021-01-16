package com.pingcap.flink.ticdc.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.flink.ticdc.KafkaMessage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TiCDCEventDecoder implements Iterator<TiCDCEvent> {

    private static final String UPDATE_NEW_VALUE_TOKEN = "u";
    private static final String UPDATE_OLD_VALUE_TOKEN = "p";

    private final DataInputStream keyStream;
    private final DataInputStream valueStream;

    private boolean hasNext = true;
    private long nextKeyLength;
    private long nextValueLength;

    private final KafkaMessage kafkaMessage;

    // visible for test
    TiCDCEventDecoder(byte[] keyBytes, byte[] valueBytes) {
        this(new KafkaMessage(keyBytes, valueBytes));
    }

    public TiCDCEventDecoder(KafkaMessage kafkaMessage) {
        this.kafkaMessage = kafkaMessage;
        keyStream = new DataInputStream(new ByteArrayInputStream(kafkaMessage.getKey()));
        readKeyVersion();
        readKeyLength();
        valueStream = new DataInputStream(new ByteArrayInputStream(kafkaMessage.getValue()));
        readValueLength();
    }

    private void readKeyLength() {
        try {
            nextKeyLength = keyStream.readLong();
            hasNext = true;
        } catch (EOFException e) {
            hasNext = false;
        } catch (Exception e) {
            throw new RuntimeException("Illegal format, can not read length", e);
        }
    }

    private void readValueLength() {
        try {
            nextValueLength = valueStream.readLong();
        } catch (EOFException e) {
            // ignore
        } catch (Exception e) {
            throw new RuntimeException("Illegal format, can not read length", e);
        }
    }


    private void readKeyVersion() {
        long version;
        try {
            version = keyStream.readLong();
        } catch (IOException e) {
            throw new RuntimeException("Illegal format, can not read version", e);
        }
        if (version != 1) {
            throw new RuntimeException("Illegal version, should be 1");
        }
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }


    public TiCDCEventKey createTidbEventKey(String json) {
        JSONObject jsonObject = JSON.parseObject(json);
        TiCDCEventKey key = new TiCDCEventKey();
        key.setTs(jsonObject.getLongValue("ts"));
        key.setT(jsonObject.getLongValue("t"));
        key.setScm(jsonObject.getString("scm"));
        key.setTbl(jsonObject.getString("tbl"));
        return key;
    }

    public TiCDCEventValue createTidbEventValue(String json) {
        // resolve
        if (json == null || json.length() == 0) {
            return new TiCDCEventResolved(kafkaMessage);
        }
        JSONObject jsonObject = JSON.parseObject(json);

        // ddl
        if (jsonObject.containsKey("q")) {
            TiCDCEventDDL ddl = new TiCDCEventDDL(kafkaMessage);
            ddl.setQ(jsonObject.getString("q"));
            ddl.setT(jsonObject.getIntValue("t"));
            return ddl;
        }

        // row change
        String updateOrDelete;
        if (jsonObject.containsKey(UPDATE_NEW_VALUE_TOKEN)) {
            updateOrDelete = UPDATE_NEW_VALUE_TOKEN;
        } else if (jsonObject.containsKey("d")) {
            updateOrDelete = "d";
        } else {
            throw new RuntimeException("Can not parse Value:" + json);
        }

        JSONObject row = jsonObject.getJSONObject(updateOrDelete);
        TiCDCEventRowChange v = new TiCDCEventRowChange(kafkaMessage);
        v.setUpdateOrDelete(updateOrDelete);
        if (v.getType() == TiCDCEventType.rowChange) {
            List<TiCDCEventColumn> columns = getTiCDCEventColumns(row);
            v.setColumns(columns);
        }

        if(UPDATE_NEW_VALUE_TOKEN.equals(updateOrDelete) ){
            row = jsonObject.getJSONObject(UPDATE_OLD_VALUE_TOKEN);
            if(row != null){
                v.setOldColumns(getTiCDCEventColumns(row));
            }
        }

        return v;
    }
    private List<TiCDCEventColumn> getTiCDCEventColumns(JSONObject row) {
        List<TiCDCEventColumn> columns = new ArrayList<>();
        if (row != null) {
            for (String col : row.keySet()) {
                JSONObject columnObj = row.getJSONObject(col);
                TiCDCEventColumn column = new TiCDCEventColumn();
                column.setH(columnObj.getBooleanValue("h"));
                column.setT(columnObj.getIntValue("t"));
                column.setV(columnObj.get("v"));
                column.setName(col);
                columns.add(column);
            }
        }
        return columns;
    }
    @Override
    public TiCDCEvent next() {
        try {
            byte[] key = new byte[(int) nextKeyLength];
            keyStream.readFully(key);
            readKeyLength();
            String keyData = new String(key, StandardCharsets.UTF_8);
            TiCDCEventKey ticdcEventKey = createTidbEventKey(keyData);

            byte[] val = new byte[(int) nextValueLength];
            valueStream.readFully(val);
            readValueLength();
            String valueData = new String(val, StandardCharsets.UTF_8);
            return new TiCDCEvent(ticdcEventKey, createTidbEventValue(valueData));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
