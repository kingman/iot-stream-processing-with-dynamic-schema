package com.google.cloud.solutions.transformation;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class DynamicMessageTableRowMapper implements SerializableFunction<TableRowWithMessageInfo, TableRow> {

    private static final long serialVersionUID = 6891268007272455587L;

    @Override
    public TableRow apply(TableRowWithMessageInfo input) {
        return input.getTableRow();
    }
}
