package com.tugalsan.api.sql.insert.server;

import module com.tugalsan.api.list;
import module com.tugalsan.api.log;
import module com.tugalsan.api.sql.cellgen;
import module com.tugalsan.api.sql.conn;
import module com.tugalsan.api.sql.sanitize;
import module java.sql;
import java.util.*;
import java.util.stream.*;

public class TS_SQLInsertExecutor {

    final private static TS_Log d = TS_Log.of(TS_SQLInsertExecutor.class);

    public TS_SQLInsertExecutor(TS_SQLConnAnchor anchor, CharSequence tableName) {
        this.anchor = anchor;
        this.tableName = tableName;
        this.colNames = TS_SQLConnColUtils.names(anchor, tableName);
        d.ci("constructor", "colNames", colNames);
    }
    final public TS_SQLConnAnchor anchor;
    final public CharSequence tableName;
    final public List<String> colNames;

    final public List<TS_SQLCellGenAbstract> cellGens = TGS_ListUtils.of();
    final public List cellVals = TGS_ListUtils.of();

    @Override
    public String toString() {//INSERT INTO table_name VALUES (value1, value2, value3, ...);
        d.ci("toString", "tableName", tableName);
        TS_SQLSanitizeUtils.sanitize(tableName);
        var sb = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES (");
        IntStream.range(0, cellVals.size()).forEachOrdered(i -> {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("?");
        });
        var stmt = sb.append(")").toString();
        d.ci("toString", stmt);
        return stmt;
    }

    private int set_fill(PreparedStatement stmt, int offset) {
        d.ci("set_fill", "colNames", colNames);
        d.ci("set_fill", "cellVals", cellVals);
        return TS_SQLConnStmtUtils.fill(stmt, colNames, cellVals, offset);
    }

    public TS_SQLConnStmtUpdateResult run() {
        return TS_SQLConnWalkUtils.update(anchor, toString(), fillStmt -> set_fill(fillStmt, 0));
    }
}
