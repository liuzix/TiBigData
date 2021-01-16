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

package com.pingcap.flink.ticdc.schema;

public class TiCDCEvent {
    private TiCDCEventKey ticdcEventKey;
    private TiCDCEventValue ticdcEventValue;

    public TiCDCEvent() {
    }

    public TiCDCEvent(TiCDCEventKey ticdcEventKey, TiCDCEventValue ticdcEventValue) {
        this.ticdcEventKey = ticdcEventKey;
        this.ticdcEventValue = ticdcEventValue;
    }

    public TiCDCEventKey getTiCDCEventKey() {
        return ticdcEventKey;
    }

    public void setTiCDCEventKey(TiCDCEventKey ticdcEventKey) {
        this.ticdcEventKey = ticdcEventKey;
    }

    public TiCDCEventValue getTiCDCEventValue() {
        return ticdcEventValue;
    }

    public void setTiCDCEventValue(TiCDCEventValue ticdcEventValue) {
        this.ticdcEventValue = ticdcEventValue;
    }

    @Override
    public String toString() {
        return "TiCDCEvent ts = " + ticdcEventKey.getTs();
    }
}