package ${package};

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.internal.MemoryRecords;

import io.kroxylicious.kafka.transform.RecordStream;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import ${package}.config.SampleFilterConfig;
import ${package}.util.SampleFilterTransformer;

/**
 * A sample FetchResponseFilter implementation, intended to demonstrate how custom filters work with
 * Kroxylicious.<br />
 * <br/>
 * This filter transforms the topic data sent by a Kafka broker in response to a fetch request by
 * replacing all occurrences of the String "bar" with the String "baz" (configurable), and by prefixing
 * the value with the name of the topic the record belongs to.<br />
 * <br/>
 * Newer Apache Kafka Fetch RPCs carry topic ids rather than topic names. This filter demonstrates the
 * preferred Kroxylicious pattern for resolving topic ids to topic names using
 * {@link FilterContext#topicNames}: collect all non-zero topic ids in the response and ask the framework
 * to resolve them in a single call. The resolved {@link TopicNameMapping} short-circuits when the input
 * set is empty, so the same code path works for older response versions that carry topic names directly.
 */
class SampleFetchResponseFilter implements FetchResponseFilter {

    private final SampleFilterConfig config;
    SampleFetchResponseFilter(SampleFilterConfig config) {
        this.config = config;
    }

    /**
     * Handle the given response, transforming the data in-place according to the configuration, forwarding
     * the FetchResponseData instance onward.
     *
     * @param apiVersion the apiVersion of the response
     * @param header     response header.
     * @param response   The KRPC message to handle.
     * @param context    The context.
     * @return CompletionStage that will yield a {@link ResponseFilterResult} containing the response to be forwarded.
     */
    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, FilterContext context) {
        List<Uuid> topicIds = response.responses().stream()
                .map(FetchResponseData.FetchableTopicResponse::topicId)
                .filter(uuid -> !Uuid.ZERO_UUID.equals(uuid))
                .toList();
        // Even when topicIds is empty, calling topicNames keeps the code path uniform; the framework
        // has a fast path for the empty case.
        return context.topicNames(topicIds).thenCompose(topicNameMapping -> {
            applyTransformation(response, context, topicNameMapping);
            return context.forwardResponse(header, response);
        });
    }

    /**
     * Applies the transformation to each partition's records, prefixing each value with the topic name
     * resolved either from the response itself or from the topic-id lookup.
     */
    private void applyTransformation(FetchResponseData response, FilterContext context, TopicNameMapping topicNameMapping) {
        response.responses().forEach(responseData -> {
            String topicName = resolveTopicName(responseData, topicNameMapping);
            for (FetchResponseData.PartitionData partitionDatum : responseData.partitions()) {
                MemoryRecords records = (MemoryRecords) partitionDatum.records();
                partitionDatum.setRecords(RecordStream.ofRecords(records)
                        .toMemoryRecords(
                                context.createByteBufferOutputStream(records.sizeInBytes()),
                                new SampleFilterTransformer(topicName, config.getFindValue(), config.getReplacementValue()){}));
            }
        });
    }

    private static String resolveTopicName(FetchResponseData.FetchableTopicResponse responseData, TopicNameMapping topicNameMapping) {
        if (responseData.topic() != null && !responseData.topic().isEmpty()) {
            return responseData.topic();
        }
        String resolved = topicNameMapping.topicNames().get(responseData.topicId());
        return resolved != null ? resolved : "unknown";
    }

}
