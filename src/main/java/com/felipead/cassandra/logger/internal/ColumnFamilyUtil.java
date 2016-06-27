package com.felipead.cassandra.logger.internal;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;

import java.nio.*;
import java.nio.charset.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

public class ColumnFamilyUtil {
    
    public static String getKeyspaceName(ColumnFamily columnFamily) {
        return columnFamily.metadata().ksName;
    }

    public static String getTableName(ColumnFamily columnFamily) {
        return columnFamily.metadata().cfName;
    }

    public static String getKeyText(ColumnFamily columnFamily, ByteBuffer key) {
        return columnFamily.metadata().getKeyValidator().getString(key);
    }

    public static Set<String> getCellNames(ColumnFamily columnFamily) {
        Set<String> cellNames = new HashSet<>();
        CFMetaData metadata = columnFamily.metadata();
        for (Cell cell : columnFamily) {
            if (cell.value().remaining() > 0) {
                String cellName = metadata.comparator.getString(cell.name());
                cellNames.add(normalizeCellName(cellName));
            }
        }
        return cellNames;
    }

    public static Set<String> getCellValues(ColumnFamily columnFamily) {
        Set<String> cellValues = new HashSet<>();
        CFMetaData metadata = columnFamily.metadata();
        for (Cell cell : columnFamily) {
            if (cell.value().remaining() > 0) {
                // need to clone it otherwise the values are not carried over to the original table
                ByteBuffer byteValue = ColumnFamilyUtil.clone(cell.value());
                CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(byteValue);
                String cellValue = charBuffer.toString();
                // String cellValue = cell.value();
                cellValues.add(cellValue);
            }
        }
        return cellValues;
    }

    public static Map<String, String> getCellMap(ColumnFamily columnFamily) {
        Map<String, String> cellMap = new HashMap<String, String>();
        CFMetaData metadata = columnFamily.metadata();
        for (Cell cell : columnFamily) {
            if (cell.value().remaining() > 0) {
                // need to clone it otherwise the values are not carried over to the original table
                ByteBuffer byteValue = ColumnFamilyUtil.clone(cell.value());
                CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(byteValue);
                String cellValue = charBuffer.toString();

                String cellName = normalizeCellName(
                    metadata.comparator.getString(cell.name())
                );

                // String cellValue = cell.value();
                cellMap.put(cellName, cellValue);
            }
        }
        return cellMap;
    }

    public static ByteBuffer clone(ByteBuffer original) {
       ByteBuffer clone = ByteBuffer.allocate(original.capacity());
       original.rewind();//copy from the beginning
       clone.put(original);
       original.rewind();
       clone.flip();
       return clone;
    }

    public static boolean isDeleted(ColumnFamily columnFamily) {
        return columnFamily.isMarkedForDelete();
    }
    
    private static String normalizeCellName(String cellName) {
        return cellName.trim().toLowerCase();
    }
}