package ${package};

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.record.internal.MemoryRecords;

import io.kroxylicious.kafka.transform.RecordStream;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import ${package}.config.SampleFilterConfig;
import ${package}.util.SampleFilterTransformer;

/**
 * A sample ProduceRequestFilter implementation, intended to demonstrate how custom filters work with
 * Kroxylicious.<br />
 * <br />
 * This filter transforms the partition data sent by a Kafka producer in a produce request by replacing all
 * occurrences of the String "foo" with the String "bar" (configurable), and by prefixing the value with
 * the name of the topic the record belongs to.<br />
 * <br />
 * Newer Apache Kafka Produce RPCs carry topic ids rather than topic names. This filter demonstrates the
 * preferred Kroxylicious pattern for resolving topic ids to topic names using
 * {@link FilterContext#topicNames}: collect all non-zero topic ids in the request and ask the framework
 * to resolve them in a single call. The resolved {@link TopicNameMapping} short-circuits when the input
 * set is empty, so the same code path works for older request versions that carry topic names directly.
 */
class SampleProduceRequestFilter implements ProduceRequestFilter {

    private final SampleFilterConfig config;

    SampleProduceRequestFilter(SampleFilterConfig config) {
        this.config = config;
    }

    /**
     * Handle the given request, transforming the data in-place according to the configuration, and returning
     * the ProduceRequestData instance to be passed to the next filter.
     *
     * @param apiVersion the apiVersion of the request
     * @param header     request header.
     * @param request    The KRPC message to handle.
     * @param context    The context.
     * @return CompletionStage that will yield a {@link RequestFilterResult} containing the request to be forwarded.
     */
    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        List<Uuid> topicIds = request.topicData().stream()
                .map(ProduceRequestData.TopicProduceData::topicId)
                .filter(uuid -> !Uuid.ZERO_UUID.equals(uuid))
                .toList();
        // Even when topicIds is empty (older API versions that already carry topic names), calling
        // topicNames keeps the code path uniform; the framework has a fast path for the empty case.
        return context.topicNames(topicIds).thenCompose(topicNameMapping -> {
            applyTransformation(request, context, topicNameMapping);
            return context.forwardRequest(header, request);
        });
    }

    /**
     * Applies the transformation to each partition's records, prefixing each value with the topic name
     * resolved either from the request itself or from the topic-id lookup.
     */
    private void applyTransformation(ProduceRequestData request, FilterContext context, TopicNameMapping topicNameMapping) {
        request.topicData().forEach(topicData -> {
            String topicName = resolveTopicName(topicData, topicNameMapping);
            for (ProduceRequestData.PartitionProduceData partitionDatum : topicData.partitionData()) {
                MemoryRecords records = (MemoryRecords) partitionDatum.records();
                partitionDatum.setRecords(RecordStream.ofRecords(records)
                        .toMemoryRecords(
                                context.createByteBufferOutputStream(records.sizeInBytes()),
                                new SampleFilterTransformer(topicName, config.getFindValue(), config.getReplacementValue()){}));
            }
        });
    }

    private static String resolveTopicName(ProduceRequestData.TopicProduceData topicData, TopicNameMapping topicNameMapping) {
        if (topicData.name() != null && !topicData.name().isEmpty()) {
            return topicData.name();
        }
        String resolved = topicNameMapping.topicNames().get(topicData.topicId());
        // Falling back to a placeholder keeps this archetype simple. A production filter should log
        // the failure and either short-circuit with an error response or drop the request; see
        // io.kroxylicious.filter.simpletransform.ProduceRequestTransformationFilter for that pattern.
        return resolved != null ? resolved : "unknown";
    }

}
