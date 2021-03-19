package com.google.cloud.solutions.common;

import com.google.api.services.bigquery.model.TableRow;

import java.io.Serializable;

public class TableRowWithMessageInfo implements Serializable {
    private static final long serialVersionUID = 5038761469411530015L;
    private final IoTCoreMessageInfo messageInfo;
    private final TableRow tableRow;

    public TableRowWithMessageInfo(final IoTCoreMessageInfo messageInfo, final TableRow tableRow) {
        this.messageInfo = messageInfo;
        this.tableRow = tableRow;
    }

    public IoTCoreMessageInfo getMessageInfo() {
        return messageInfo;
    }

    public TableRow getTableRow() {
        return tableRow;
    }
}
