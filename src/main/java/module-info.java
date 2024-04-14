module com.tugalsan.api.sql.insert {
    requires java.sql;
    requires com.tugalsan.api.runnable;
    requires com.tugalsan.api.union;
    requires com.tugalsan.api.stream;
    requires com.tugalsan.api.string;
    requires com.tugalsan.api.time;
    requires com.tugalsan.api.list;
    requires com.tugalsan.api.callable;
    requires com.tugalsan.api.log;
    requires com.tugalsan.api.file.obj;
    requires com.tugalsan.api.sql.col.typed;
    requires com.tugalsan.api.sql.cell;
    requires com.tugalsan.api.sql.cellgen;
    requires com.tugalsan.api.sql.conn;
    requires com.tugalsan.api.sql.sanitize;
    exports com.tugalsan.api.sql.insert.server;
}
