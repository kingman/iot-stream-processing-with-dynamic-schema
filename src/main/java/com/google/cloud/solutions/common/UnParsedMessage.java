package com.google.cloud.solutions.common;

import java.io.Serializable;

public class UnParsedMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private IoTCoreMessageInfo messageInfo;
    private String message;

    public IoTCoreMessageInfo getMessageInfo() {
        return messageInfo;
    }

    public void setMessageInfo(IoTCoreMessageInfo messageInfo) {
        this.messageInfo = messageInfo;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}