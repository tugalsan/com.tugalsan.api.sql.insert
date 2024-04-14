package com.tugalsan.api.sql.insert.server;

import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.union.client.TGS_UnionExcuse;

public class TS_SQLInsertUtils {

//    final private static TS_Log d = TS_Log.of(TS_SQLInsertUtils.class);
    public static TGS_UnionExcuse<TS_SQLInsert> insert(TS_SQLConnAnchor anchor, CharSequence tableName) {
        return TS_SQLInsert.of(anchor, tableName);
    }

//    public static void test() {
//        TS_SQLInsertUtils.insert(null, "tn").valObj("12");
//    }
}
