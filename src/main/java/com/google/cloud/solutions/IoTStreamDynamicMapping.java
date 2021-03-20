package com.google.cloud.solutions;

import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.common.UnParsedMessage;
import com.google.cloud.solutions.transformation.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class IoTStreamDynamicMapping {
    public interface IoTStreamDynamicMappingOptions extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        IoTStreamDynamicMapping.IoTStreamDynamicMappingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(IoTStreamDynamicMapping.IoTStreamDynamicMappingOptions.class);
        options.setStreaming(true);

        final TupleTag<PubSubMessageWithMessageInfo> knownMessageTag = new TupleTag<PubSubMessageWithMessageInfo>() {};
        final TupleTag<PubSubMessageWithMessageInfo> unknownMessageTag = new TupleTag<PubSubMessageWithMessageInfo>() {};

        Pipeline pipeline = Pipeline.create(options);
        PCollectionTuple pCollectionTuple = pipeline
                .apply("Read IoT Core events", PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()))
                .apply("Validate message schema", ParDo.of(new IoTMessageSchemaValidation(knownMessageTag, unknownMessageTag))
                        .withOutputTags(knownMessageTag, TupleTagList.of(unknownMessageTag)));

        processKnownMessages(pCollectionTuple.get(knownMessageTag));
        processUnknownMessages(pCollectionTuple.get(unknownMessageTag));

        pipeline.run();
    }

    private static void processUnknownMessages(PCollection<PubSubMessageWithMessageInfo> unknownMessages) {
        unknownMessages
                .apply("Extract the unknown message", ParDo.of(new PubSubMessageToUnParsedMessage()))
                .apply("Store message to BigQuery", BigQueryIO.<UnParsedMessage>write()
                        .to(new UnParsedMessageToTableDestination())
                        .withFormatFunction(new UnParsedMessageTableRowMapper())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    }

    private static void processKnownMessages(PCollection<PubSubMessageWithMessageInfo> messages) {
        messages
                .apply("Convert message to table row", ParDo.of(new PubSubMessageToTableRowMapper()))
                .apply("Store message to BigQuery", BigQueryIO.<TableRowWithMessageInfo>write()
                .to(new DynamicMessageToTableDestination())
                .withFormatFunction(new DynamicMessageTableRowMapper())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    }
}