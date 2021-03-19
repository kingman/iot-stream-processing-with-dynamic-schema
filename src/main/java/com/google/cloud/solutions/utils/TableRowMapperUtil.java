package com.google.cloud.solutions.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.common.*;

public class TableRowMapperUtil {

    private static void mapMessageInfo(IoTCoreMessageInfo messageInfo, TableRow tableRow) {
        tableRow
        .set("DeviceNumId", messageInfo.getDeviceNumId())
        .set("DeviceId", messageInfo.getDeviceId())
        .set("RegistryId", messageInfo.getDeviceRegistryId());
    }

    public static void mapUnParsedMessage(UnParsedMessage message, TableRow tableRow) {
        mapMessageInfo(message.getMessageInfo(), tableRow);
        tableRow.set("Message", message.getMessage());
    }
}