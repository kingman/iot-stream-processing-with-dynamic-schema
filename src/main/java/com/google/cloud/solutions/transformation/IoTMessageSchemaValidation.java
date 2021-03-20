package com.google.cloud.solutions.transformation;

import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.utils.InputDataSchemaValidator;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class IoTMessageSchemaValidation extends DoFn<PubsubMessage, PubSubMessageWithMessageInfo> {

    private static final long serialVersionUID = 1L;
    private final TupleTag<PubSubMessageWithMessageInfo> knownMessageTag;
    private final TupleTag<PubSubMessageWithMessageInfo> unknownMessageTag;
    private final InputDataSchemaValidator inputDataSchemaValidator;


    public IoTMessageSchemaValidation(TupleTag<PubSubMessageWithMessageInfo> knownMessageTag, TupleTag<PubSubMessageWithMessageInfo> unknownMessageTag) {
        this.knownMessageTag = knownMessageTag;
        this.unknownMessageTag = unknownMessageTag;
        this.inputDataSchemaValidator = new InputDataSchemaValidator();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        PubSubMessageWithMessageInfo messageWithInfo = new PubSubMessageWithMessageInfo(context.element());

        if(inputDataSchemaValidator.apply(messageWithInfo)) {
            context.output(knownMessageTag, messageWithInfo);
        } else {
            context.output(unknownMessageTag, messageWithInfo);
        }
    }
}
