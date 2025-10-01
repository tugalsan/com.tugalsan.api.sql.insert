package com.tugalsan.api.sql.insert.server;

import module com.tugalsan.api.sql.cellgen;

public class TS_SQLInsertGen {

    public TS_SQLInsertGen(TS_SQLInsertExecutor executor) {
        this.executor = executor;
    }
    final private TS_SQLInsertExecutor executor;

    public TS_SQLInsertGen setBytesDefault(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenBytesDefault(executor, colIdx));
        return this;
    }

    public TS_SQLInsertGen setBytesStrDefault(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenBytesStrDefault(executor, colIdx));
        return this;
    }

    public TS_SQLInsertGen setLngDateCur(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenLngDateCur(executor, colIdx));
        return this;
    }

    public TS_SQLInsertGen setLngDateDefault(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenLngDateDefault(executor, colIdx));
        return this;
    }

    public TS_SQLInsertGen setLngDefault(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenLngDefault(executor, colIdx));
        return this;
    }

    public TS_SQLInsertGen setLngNext(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenLngNext(executor, colIdx, executor.anchor, executor.tableName, executor.colNames));
        return this;
    }

    public TS_SQLInsertGen setLngNextDated(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenLngNextDated(executor, colIdx, executor.anchor, executor.tableName, executor.colNames));
        return this;
    }

    public TS_SQLInsertGen setLngTimeCur(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenLngTimeCur(executor, colIdx));
        return this;
    }

    public TS_SQLInsertGen setStrDefault(int colIdx) {
        executor.cellGens.set(colIdx, new TS_SQLCellGenStrDefault(executor, colIdx));
        return this;
    }
}
