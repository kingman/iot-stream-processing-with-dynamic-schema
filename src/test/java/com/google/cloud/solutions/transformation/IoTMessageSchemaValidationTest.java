package com.google.cloud.solutions.transformation;

import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.utils.InputDataSchemaValidator;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class IoTMessageSchemaValidationTest {
    private static final PubsubMessage[] messages = new PubsubMessage[]{
            new PubsubMessage("valid1".getBytes(),null),
            new PubsubMessage("valid2".getBytes(),null),
            new PubsubMessage("invalid".getBytes(),null),

    };
    private static final List<PubsubMessage> INPUT_MESSAGES = Arrays.asList(messages);

    @Test
    public void testTransform() {
        final TupleTag<PubSubMessageWithMessageInfo> knownMessageTag =
                new TupleTag<PubSubMessageWithMessageInfo>() {
                };
        final TupleTag<PubSubMessageWithMessageInfo> unknownMessageTag =
                new TupleTag<PubSubMessageWithMessageInfo>() {
                };
        try (MockedConstruction<InputDataSchemaValidator> mocked = Mockito.mockConstruction(InputDataSchemaValidator.class,
                (mock, context) -> {
                    when(mock.apply(any(PubSubMessageWithMessageInfo.class))).thenAnswer(new Answer<Object>() {
                        private int count = 0;

                        @Override
                        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                            if (count++ > 1) {
                                return false;
                            } else {
                                return true;
                            }
                        }
                    });
                })) {
            IoTMessageSchemaValidation cut = new IoTMessageSchemaValidation(knownMessageTag, unknownMessageTag);
            TestPipeline testPipeline = TestPipeline.create();
            PCollection<PubsubMessage> input = testPipeline.apply(Create.of(INPUT_MESSAGES));
            PCollectionTuple output = input.apply(ParDo.of(cut).withOutputTags(knownMessageTag, TupleTagList.of(unknownMessageTag)));

            PCollection<Long> numberOfKnownMessages = output.get(knownMessageTag).apply(Count.globally());
            PAssert.that(numberOfKnownMessages).

            assertEquals(2, numberOfKnownMessages);
            PCollection<Long> numberOfUnknownMessages = output.get(unknownMessageTag).apply(Count.globally());
            assertEquals(1, numberOfUnknownMessages);
            testPipeline.run();
        }
    }
}
