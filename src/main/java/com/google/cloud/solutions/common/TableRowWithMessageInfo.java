package com.google.cloud.solutions.common;

import java.io.Serializable;
import java.util.Map;

public class TableRowWithMessageInfo implements Serializable {
    private static final long serialVersionUID = 5038761469411530015L;
    private final IoTCoreMessageInfo messageInfo;
    private final Map<String, String> tableRowMap;

    public TableRowWithMessageInfo(final IoTCoreMessageInfo messageInfo, final Map<String, String> tableRowMap) {
        this.messageInfo = messageInfo;
        this.tableRowMap = tableRowMap;
    }

    public IoTCoreMessageInfo getMessageInfo() {
        return messageInfo;
    }

    public Map<String, String> getTableRowMap() {
        return tableRowMap;
    }
}
