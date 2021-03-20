package com.google.cloud.solutions.transformation;

import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.common.UnParsedMessage;

import org.apache.beam.sdk.transforms.DoFn;

public class PubSubMessageToUnParsedMessage extends DoFn<PubSubMessageWithMessageInfo, UnParsedMessage> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(@Element PubSubMessageWithMessageInfo message, OutputReceiver<UnParsedMessage> receiver) {
        UnParsedMessage unParsedMessage = new UnParsedMessage();
        unParsedMessage.setMessageInfo(message.getMessageInfo());
        unParsedMessage.setMessage(new String(message.getMessage().getPayload()));
        receiver.output(unParsedMessage);
    }
}