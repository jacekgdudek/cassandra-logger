package com.felipead.cassandra.logger.log;

import com.felipead.cassandra.logger.internal.ColumnFamilyUtil;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.utils.UUIDGen;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import java.util.Map;
import java.util.UUID;

public class LogEntryBuilder {

    private Collection<String> ignoreColumns;
    
    public void setIgnoreColumns(Collection<String> ignoreColumns) {
        this.ignoreColumns = ignoreColumns;
    }

    public LogEntry build(ColumnFamily update, ByteBuffer key) {
        LogEntry logEntry = new LogEntry();

        logEntry.setTimeUuid(generateTimeUuid());
        logEntry.setLoggedKeyspace(ColumnFamilyUtil.getKeyspaceName(update));
        logEntry.setLoggedTable(ColumnFamilyUtil.getTableName(update));
        logEntry.setLoggedKey(ColumnFamilyUtil.getKeyText(update, key));

        if (ColumnFamilyUtil.isDeleted(update)) {
            logEntry.setOperation(Operation.delete);
        } else {
            logEntry.setOperation(Operation.save);
        }

        Map<String, String> updatedCells = ColumnFamilyUtil.getCellMap(update);
        if (this.ignoreColumns != null) {
            updatedCells.keySet().removeAll(this.ignoreColumns);
        }
        
        logEntry.setUpdatedColumns(updatedCells);
        return logEntry;
    }

    private static UUID generateTimeUuid() {
        return UUIDGen.getTimeUUID();
    }
}