package com.tugalsan.api.sql.insert.server;

import com.tugalsan.api.callable.client.TGS_CallableType1Void;
import java.util.*;
import java.util.stream.*;

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
import com.tugalsan.api.unsafe.client.*;

public class TS_SQLInsert {

    final private static TS_Log d = TS_Log.of(TS_SQLInsert.class);

    public TS_SQLInsert(TS_SQLConnAnchor anchor, CharSequence tableName) {
        executor = new TS_SQLInsertExecutor(anchor, tableName);
    }
    final private TS_SQLInsertExecutor executor;

    private TS_SQLConnStmtUpdateResult valDriver(TGS_CallableType1Void<List> vals) {
        vals.run(executor.cellVals);
        return executor.run();
    }

    public TS_SQLConnStmtUpdateResult valCell(TGS_SQLCellAbstract... vals) {
        return valCell(TGS_ListUtils.of(vals));
    }

    public TS_SQLConnStmtUpdateResult valCell(List<TGS_SQLCellAbstract> vals) {
        return valDriver(valss -> {
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
                TGS_UnSafe.thrw(d.className, "valCell(List<TGS_SQLCellAbstract> vals)", "Unknown cell type");
            });
        });
    }

    public TS_SQLConnStmtUpdateResult valObj(Object... vals) {
        if (vals.length == 0) {
            return TS_SQLConnStmtUpdateResult.of(0, null);
        }
//        if (vals[0] instanceof List) {//recursive error, WHY?
//            return Arrays.asList(vals).stream().mapToInt(rows -> valObj(rows)).sum();
//        }
        return valObj(TGS_ListUtils.of(vals));
    }

    public TS_SQLConnStmtUpdateResult valObj(List<Object> vals) {
        if (vals.isEmpty()) {
            return TS_SQLConnStmtUpdateResult.of(0, null);
        }
//        if (vals.get(0) instanceof List) {//recursive error, WHY?
//            return vals.stream().mapToInt(rows -> valObj(rows)).sum();
//        }
        return valCell(TGS_StreamUtils.toLst(
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
                        return TGS_UnSafe.thrw(d.className, "valObj(List<Object> vals)", "Long/Integer/short/TGS_Time cell should be supplied for familyLng. o: " + o.getClass().getSimpleName() + " -> " + o);
                    }
                    if (ct.familyStr()) {
                        if (o instanceof CharSequence val) {
                            return new TGS_SQLCellSTR(val);
                        }
                        d.ce("List<Object> vals", "tableName", executor.tableName);
                        d.ce("List<Object> vals", "cols", executor.colNames);
                        d.ce("List<Object> vals", "vals", vals);
                        return TGS_UnSafe.thrw(d.className, "valObj(List<Object> vals)", "CharSequence cell should be supplied for familyStr. o: " + o.getClass().getSimpleName() + " -> " + o);
                    }
                    if (ct.typeBytesStr()) {
                        if (o instanceof CharSequence val) {
                            return new TGS_SQLCellSTR(val);
                        }
                        d.ce("List<Object> vals", "tableName", executor.tableName);
                        d.ce("List<Object> vals", "cols", executor.colNames);
                        d.ce("List<Object> vals", "vals", vals);
                        return TGS_UnSafe.thrw(d.className, "valObj(List<Object> vals)", "CharSequence cell should be supplied for typeBytesStr. o: " + o.getClass().getSimpleName() + " -> " + o);
                    }
                    if (ct.familyBytes()) {
                        if (o instanceof Object[] && ct.typeBytesRow()) {
                            var val = TS_FileObjUtils.toBytes((Object[]) o).orElse(e -> new byte[0]);
                            return new TGS_SQLCellBYTES(val);
                        }
                        if (o instanceof byte[] val) {
                            return new TGS_SQLCellBYTES(val);
                        }
                        var val = TS_FileObjUtils.toBytes(o).orElse(e -> new byte[0]);
                        return new TGS_SQLCellBYTES(val);
                    }
                    d.ce("List<Object> vals", "tableName", executor.tableName);
                    d.ce("List<Object> vals", "cols", executor.colNames);
                    d.ce("List<Object> vals", "vals", vals);
                    return TGS_UnSafe.thrw(d.className, "valObj(List<Object> vals)", "Unknown colummn type cn: " + o.getClass().getSimpleName() + " -> " + cn);
                })
        ));
    }

    public TS_SQLConnStmtUpdateResult gen_then_setCell(TGS_CallableType1Void<TS_SQLInsertGen> gen) {
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
            d.ce("gen(TGS_CallableType1Void<TS_SQLInsertGen> gen)", "tableName", executor.tableName);
            d.ce("gen(TGS_CallableType1Void<TS_SQLInsertGen> gen)", "cols", executor.colNames);
            TGS_UnSafe.thrw(d.className, "gen", "unknown colun generation type");
        });
        var g = new TS_SQLInsertGen(executor);
        gen.run(g);
        return executor.run();
    }
}
