package com.google.cloud.solutions.transformation;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.utils.TableSchemaLoader;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

import java.util.Map;

public class DynamicMessageTableRowMapper implements SerializableFunction<TableRowWithMessageInfo, TableRow> {

    private static final long serialVersionUID = 6891268007272455587L;

    @Override
    public TableRow apply(TableRowWithMessageInfo input) {
        Map<String, TableFieldSchema> tableSchema = TableSchemaLoader.getFieldMap(input.getMessageInfo());
        return toTableRow(input.getTableRowMap(), tableSchema);
    }

    private TableRow toTableRow(Map<String, String> row, Map<String, TableFieldSchema> tableSchema) {
        TableRow tableRow = new TableRow();
        for(String key: row.keySet()) {
            if(tableSchema.containsKey(key)) {
                TableFieldSchema tableFieldSchema = tableSchema.get(key);
                tableRow.put(key, createTableRowField(row.get(key), tableFieldSchema));
            }
        }
        return tableRow;
    }

    private Object createTableRowField(String valueStr, TableFieldSchema tableFieldSchema) {
        if("string".equalsIgnoreCase(tableFieldSchema.getType())) {
            return valueStr;
        }
        if("int64".equalsIgnoreCase(tableFieldSchema.getType())) {
            return Long.parseLong(valueStr);
        }
        if("timestamp".equalsIgnoreCase(tableFieldSchema.getType())) {
            try {
                long epochMilli = Long.parseLong(valueStr);
                if (valueStr.length() > 13) {
                    epochMilli = Long.parseLong(valueStr.substring(0,13));
                }
                return Instant.ofEpochMilli(epochMilli).toString();
            } catch (NumberFormatException nfe) {
                return valueStr;
            }
        }
        if("float".equalsIgnoreCase(tableFieldSchema.getType())) {
            return Double.parseDouble(valueStr);
        }
        return valueStr;
    }
}
