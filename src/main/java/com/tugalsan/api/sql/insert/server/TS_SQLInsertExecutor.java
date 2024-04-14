package com.tugalsan.api.sql.insert.server;

import java.util.*;
import java.util.stream.*;
import java.sql.*;
import com.tugalsan.api.list.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.sql.cellgen.server.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.union.client.TGS_UnionExcuse;

public class TS_SQLInsertExecutor {

    final private static TS_Log d = TS_Log.of(TS_SQLInsertExecutor.class);

    protected TS_SQLInsertExecutor(TS_SQLConnAnchor anchor, CharSequence tableName, List<String> colNames) {
        this.anchor = anchor;
        this.tableName = tableName;
        this.colNames = colNames;
        d.ci("constructor", "colNames", colNames);
    }

    public static TGS_UnionExcuse<TS_SQLInsertExecutor> of(TS_SQLConnAnchor anchor, CharSequence tableName) {
        var u = TS_SQLConnColUtils.names(anchor, tableName);
        if (u.isExcuse()) {
            return u.toExcuse();
        }
        return TGS_UnionExcuse.of(new TS_SQLInsertExecutor(anchor, tableName, u.value()));
    }

    final public TS_SQLConnAnchor anchor;
    final public CharSequence tableName;
    final public List<String> colNames;

    final public List<TS_SQLCellGenAbstract> cellGens = TGS_ListUtils.of();
    final public List cellVals = TGS_ListUtils.of();

    @Override
    public String toString() {
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

    private TGS_UnionExcuse<Integer> set_fill(PreparedStatement stmt, int offset) {
        d.ci("set_fill", "colNames", colNames);
        d.ci("set_fill", "cellVals", cellVals);
        return TS_SQLConnStmtUtils.fill(stmt, colNames, cellVals, offset);
    }

    public TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> run() {
        var wrap = new Object() {
            TGS_UnionExcuse<Integer> u_fill;
        };
        var u_update = TS_SQLConnWalkUtils.update(anchor, toString(), fillStmt -> {
            wrap.u_fill = set_fill(fillStmt, 0);
        });
        if (wrap.u_fill != null && wrap.u_fill.isExcuse()) {
            return wrap.u_fill.toExcuse();
        }
        return u_update;
    }
}
