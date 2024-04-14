package com.tugalsan.api.sql.insert.server;

import java.util.*;
import java.util.stream.*;
import com.tugalsan.api.runnable.client.*;
import com.tugalsan.api.file.obj.server.*;
import com.tugalsan.api.list.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.sql.cell.client.*;
import com.tugalsan.api.sql.cellgen.server.*;
import com.tugalsan.api.sql.col.typed.client.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.stream.client.*;
import com.tugalsan.api.string.client.*;
import com.tugalsan.api.string.server.*;
import com.tugalsan.api.time.client.*;
import com.tugalsan.api.union.client.TGS_UnionExcuse;

public class TS_SQLInsert {

    final private static TS_Log d = TS_Log.of(TS_SQLInsert.class);

    public TS_SQLInsert(TS_SQLConnAnchor anchor, CharSequence tableName, List<String> colNames) {
        executor = new TS_SQLInsertExecutor(anchor, tableName, colNames);
    }
    final private TS_SQLInsertExecutor executor;

    public static TGS_UnionExcuse<TS_SQLInsert> of(TS_SQLConnAnchor anchor, CharSequence tableName) {
        var u = TS_SQLConnColUtils.names(anchor, tableName);
        if (u.isExcuse()) {
            return u.toExcuse();
        }
        return TGS_UnionExcuse.of(new TS_SQLInsert(anchor, tableName, u.value()));
    }

    private TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> valDriver(TGS_RunnableType1<List> vals) {
        vals.run(executor.cellVals);
        return executor.run();
    }

    public TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> valCell(TGS_SQLCellAbstract... vals) {
        return valCell(TGS_ListUtils.of(vals));
    }

    public TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> valCell(List<TGS_SQLCellAbstract> vals) {
        var wrap = new Object() {
            TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> inner;
        };
        var u_driver = valDriver(valss -> {
            IntStream.range(0, vals.size()).forEachOrdered(i -> {
                if (vals.get(i) instanceof TGS_SQLCellBYTESSTR cell) {
                    var valString = cell.getValueString();
                    var bytes = TS_StringUtils.toByte(TGS_StringUtils.toEmptyIfNull(valString));
                    valss.add(bytes);
                    return;
                }
                if (vals.get(i) instanceof TGS_SQLCellSTR cell) {
                    var valString = cell.getValueString();
                    valss.add(TGS_StringUtils.toEmptyIfNull(valString));
                    return;
                }
                if (vals.get(i) instanceof TGS_SQLCellLNG cell) {
                    var valLong = cell.getValueLong();
                    valss.add(valLong);
                    return;
                }
                if (vals.get(i) instanceof TGS_SQLCellBYTES cell) {
                    var bytes = cell.getValueBytes();
                    valss.add(bytes);
                    return;
                }
                d.ce("valCell(List<TGS_SQLCellAbstract> vals", "tableName", executor.tableName);
                d.ce("valCell(List<TGS_SQLCellAbstract> vals", "cols", executor.colNames);
                d.ce("valCell(List<TGS_SQLCellAbstract> vals", "vals", vals);
                wrap.inner = TGS_UnionExcuse.ofExcuse(d.className, "valCell(List<TGS_SQLCellAbstract> vals)", "Unknown cell type");
            });
        });
        if (wrap.inner != null && wrap.inner.isExcuse()) {
            return wrap.inner;
        }
        return u_driver;
    }

    public TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> valObj(Object... vals) {
        if (vals.length == 0) {
            return TGS_UnionExcuse.ofExcuse(d.className, "valObj", "vals.length == 0");
        }
        return valObj(TGS_ListUtils.of(vals));
    }

    public TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> valObj(List<Object> vals) {
        if (vals.isEmpty()) {
            return TGS_UnionExcuse.ofExcuse(d.className, "valObj", "vals.length == 0");
        }
        var wrap = new Object() {
            TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> inner;
        };
        var u_cell = valCell(TGS_StreamUtils.toLst(
                IntStream.range(0, vals.size()).mapToObj(i -> {
                    var cn = executor.colNames.get(i);
                    var ct = new TGS_SQLColTyped(cn);
                    var o = vals.get(i);
                    if (ct.familyLng()) {
                        if (o instanceof Long type) {
                            return new TGS_SQLCellLNG(type);
                        }
                        if (o instanceof Integer type) {
                            var val = type.longValue();
                            return new TGS_SQLCellLNG(val);
                        }
                        if (o instanceof Short type) {
                            var val = type.longValue();
                            return new TGS_SQLCellLNG(val);
                        }
                        if (ct.typeLngDate() && o instanceof TGS_Time type) {
                            var val = type.getDate();
                            return new TGS_SQLCellLNG(val);
                        }
                        if (ct.typeLngTime() && o instanceof TGS_Time type) {
                            var val = type.getTime();
                            return new TGS_SQLCellLNG(val);
                        }
                        d.ce("List<Object> vals", "tableName", executor.tableName);
                        d.ce("List<Object> vals", "cols", executor.colNames);
                        d.ce("List<Object> vals", "vals", vals);
                        wrap.inner = TGS_UnionExcuse.ofExcuse(d.className, "valObj(List<Object> vals)", "Long/Integer/short/TGS_Time cell should be supplied for familyLng. o: " + o.getClass().getSimpleName() + " -> " + o);
                        return new TGS_SQLCellLNG(0);//bogus
                    }
                    if (ct.familyStr()) {
                        if (o instanceof CharSequence val) {
                            return new TGS_SQLCellSTR(val);
                        }
                        d.ce("List<Object> vals", "tableName", executor.tableName);
                        d.ce("List<Object> vals", "cols", executor.colNames);
                        d.ce("List<Object> vals", "vals", vals);
                        wrap.inner = TGS_UnionExcuse.ofExcuse(d.className, "valObj(List<Object> vals)", "CharSequence cell should be supplied for familyStr. o: " + o.getClass().getSimpleName() + " -> " + o);
                        return new TGS_SQLCellLNG(0);//bogus
                    }
                    if (ct.typeBytesStr()) {
                        if (o instanceof CharSequence val) {
                            return new TGS_SQLCellSTR(val);
                        }
                        d.ce("List<Object> vals", "tableName", executor.tableName);
                        d.ce("List<Object> vals", "cols", executor.colNames);
                        d.ce("List<Object> vals", "vals", vals);
                        wrap.inner = TGS_UnionExcuse.ofExcuse(d.className, "valObj(List<Object> vals)", "CharSequence cell should be supplied for typeBytesStr. o: " + o.getClass().getSimpleName() + " -> " + o);
                        return new TGS_SQLCellLNG(0);//bogus
                    }
                    if (ct.familyBytes()) {
                        if (o instanceof Object[] && ct.typeBytesRow()) {
                            var u_val = TS_FileObjUtils.toBytes((Object[]) o);
                            if (u_val.isExcuse()) {//.orElse(excuse -> new byte[0]);
                                wrap.inner = u_val.toExcuse();
                            }
                            return new TGS_SQLCellBYTES(u_val.value());
                        }
                        if (o instanceof byte[] val) {
                            return new TGS_SQLCellBYTES(val);
                        }
                        var u_val = TS_FileObjUtils.toBytes( o);
                        if (u_val.isExcuse()) {//.orElse(excuse -> new byte[0]);
                            wrap.inner = u_val.toExcuse();
                        }
                        return new TGS_SQLCellBYTES(u_val.value());
                    }
                    d.ce("List<Object> vals", "tableName", executor.tableName);
                    d.ce("List<Object> vals", "cols", executor.colNames);
                    d.ce("List<Object> vals", "vals", vals);
                    wrap.inner = TGS_UnionExcuse.ofExcuse(d.className, "valObj(List<Object> vals)", "Unknown colummn type cn: " + o.getClass().getSimpleName() + " -> " + cn);
                    return new TGS_SQLCellLNG(0);//bogus
                })
        ));
        if (wrap.inner != null && wrap.inner.isExcuse()) {
            return wrap.inner;
        }
        return u_cell;
    }

    public TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> gen_then_setCell(TGS_RunnableType1<TS_SQLInsertGen> gen) {
        var wrap = new Object() {
            TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> inner;
        };
        IntStream.range(0, executor.colNames.size()).forEachOrdered(ci -> {
            if (ci == 0) {
                var g = new TS_SQLCellGenLngNext(
                        executor, 0, executor.anchor,
                        executor.tableName, executor.colNames
                );
                executor.cellGens.add(g);
                return;
            }
            if (TGS_SQLColTypedUtils.typeLngDate(executor.colNames.get(ci))) {
                var g = new TS_SQLCellGenLngDateDefault(executor, ci);
                executor.cellGens.add(g);
                return;
            }
            if (TGS_SQLColTypedUtils.familyLng(executor.colNames.get(ci))) {
                var g = new TS_SQLCellGenLngDefault(executor, ci);
                executor.cellGens.add(g);
                return;
            }
            if (TGS_SQLColTypedUtils.familyStr(executor.colNames.get(ci))) {
                var g = new TS_SQLCellGenStrDefault(executor, ci);
                executor.cellGens.add(g);
                return;
            }
            if (TGS_SQLColTypedUtils.typeBytesStr(executor.colNames.get(ci))) {
                var g = new TS_SQLCellGenBytesStrDefault(executor, ci);
                executor.cellGens.add(g);
                return;
            }
            if (TGS_SQLColTypedUtils.familyBytes(executor.colNames.get(ci))) {
                var g = new TS_SQLCellGenBytesDefault(executor, ci);
                executor.cellGens.add(g);
                return;
            }
            d.ce("gen(TGS_RunnableType1<TS_SQLInsertGen> gen)", "tableName", executor.tableName);
            d.ce("gen(TGS_RunnableType1<TS_SQLInsertGen> gen)", "cols", executor.colNames);
            wrap.inner = TGS_UnionExcuse.ofExcuse(d.className, "gen", "unknown colun generation type");
        });
        if (wrap.inner != null && wrap.inner.isExcuse()) {
            return wrap.inner;
        }
        var g = new TS_SQLInsertGen(executor);
        gen.run(g);
        return executor.run();
    }
}
