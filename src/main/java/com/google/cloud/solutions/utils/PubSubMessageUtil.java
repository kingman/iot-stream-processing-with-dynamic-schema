package com.google.cloud.solutions.utils;

import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

public class PubSubMessageUtil {
    public static IoTCoreMessageInfo extractIoTCoreMessageInfo(PubsubMessage message) {
        IoTCoreMessageInfo messageInfo = new IoTCoreMessageInfo();
        messageInfo.setDeviceNumId(message.getAttribute("deviceNumId"));
        messageInfo.setDeviceId(message.getAttribute("deviceId"));
        messageInfo.setDeviceRegistryId(message.getAttribute("deviceRegistryId"));
        messageInfo.setDeviceRegistryLocation(message.getAttribute("deviceRegistryLocation"));
        messageInfo.setProjectId(message.getAttribute("projectId"));
        messageInfo.setSubFolder(message.getAttribute("subFolder"));
        messageInfo.setMessageType(message.getAttribute("iotMessageType"));
        return messageInfo;
    }
}