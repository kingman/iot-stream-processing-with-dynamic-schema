package com.google.cloud.solutions.transformation;

import com.google.cloud.solutions.utils.InputDataSchemaValidator;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class IoTMessageSchemaValidation extends DoFn<PubsubMessage, PubsubMessage> {

    private static final long serialVersionUID = 1L;
    private final TupleTag<PubsubMessage> knownMessageTag;
    private final TupleTag<PubsubMessage> unknownMessageTag;
    private final InputDataSchemaValidator inputDataSchemaValidator;


    public IoTMessageSchemaValidation(TupleTag<PubsubMessage> knownMessageTag, TupleTag<PubsubMessage> unknownMessageTag) {
        this.knownMessageTag = knownMessageTag;
        this.unknownMessageTag = unknownMessageTag;
        this.inputDataSchemaValidator = new InputDataSchemaValidator();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        if(inputDataSchemaValidator.apply(context.element())) {
            context.output(knownMessageTag, context.element());
        } else {
            context.output(unknownMessageTag, context.element());
        }
    }
}
