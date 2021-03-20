package com.google.cloud.solutions.common;

import com.google.cloud.solutions.utils.PubSubMessageUtil;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import java.io.Serializable;

public class PubSubMessageWithMessageInfo implements Serializable {
    private static final long serialVersionUID = -6308181018759264260L;
    private final IoTCoreMessageInfo messageInfo;
    private final PubsubMessage pubsubMessage;

    public PubSubMessageWithMessageInfo(final PubsubMessage pubsubMessage) {
        this.messageInfo = PubSubMessageUtil.extractIoTCoreMessageInfo(pubsubMessage);
        this.pubsubMessage = pubsubMessage;
    }

    public PubsubMessage getMessage() {
        return pubsubMessage;
    }

    public IoTCoreMessageInfo getMessageInfo() {
        return messageInfo;
    }
}
