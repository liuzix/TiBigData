package com.pingcap.flink.ticdc;

import com.zhihu.tibigdata.tidb.ClientConfig;
import com.zhihu.tibigdata.tidb.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.meta.TiTimestamp;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TiCDCUtils {
    static final Logger LOG = LoggerFactory.getLogger(TiCDCUtils.class);

    public static TiTimestamp getCurrentTs(Map<String, String> properties) throws Exception {
        ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
        TiTimestamp ret = clientSession.getTimestampFromPD();
        clientSession.close();
        return ret;
    }

    public static List<TiColumnInfo> getTiDBSchema(Map<String, String> properties, String tableName) {
        ClientSession clientSession = ClientSession.create(new ClientConfig(properties));
        String[] names = tableName.split("\\.");
        if (names.length != 2) {
            throw new RuntimeException("Unexpected tableName " + tableName);
        }

        Optional<TiTableInfo> info = clientSession.getTable(names[0], names[1]);
        if (!info.isPresent()) {
            throw new RuntimeException("Cannot retrieve schema from TiDB");
        }

        return info.get().getColumns();
    }
}
