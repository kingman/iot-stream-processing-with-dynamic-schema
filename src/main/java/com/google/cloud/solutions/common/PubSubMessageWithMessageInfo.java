package com.google.cloud.solutions.common;

import com.google.cloud.solutions.utils.PubSubMessageUtil;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import java.io.Serializable;

public class PubSubMessageWithMessageInfo implements Serializable {
    private static final long serialVersionUID = -6308181018759264260L;
    private final IoTCoreMessageInfo messageInfo;
    private final byte[] payload;

    public PubSubMessageWithMessageInfo(final PubsubMessage pubsubMessage) {
        this.messageInfo = PubSubMessageUtil.extractIoTCoreMessageInfo(pubsubMessage);
        this.payload = pubsubMessage.getPayload();
    }

    public byte[] getPayload() {
        return payload;
    }

    public IoTCoreMessageInfo getMessageInfo() {
        return messageInfo;
    }
}
