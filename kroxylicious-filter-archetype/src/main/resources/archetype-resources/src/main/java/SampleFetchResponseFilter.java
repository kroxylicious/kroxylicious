package ${package};

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import ${package}.config.SampleFilterConfig;
import ${package}.util.SampleFilterTransformer;

/**
 * A sample FetchResponseFilter implementation, intended to demonstrate how custom filters work with
 * Kroxylicious.<br />
 * <br/>
 * This filter transforms the topic data sent by a Kafka broker in response to a fetch request sent by a
 * Kafka consumer, by replacing all occurrences of the String "bar" with the String "baz". These strings are
 * configurable in the config file, so you could substitute this with any text you want.<br/>
 * <br/>
 * An example of a use case where this might be applicable is when producers are sending data to Kafka
 * using different formats from what consumers are expecting. You could configure this filter to transform
 * the data sent by Kafka to the consumers into the format they expect. In this example use case, the filter
 * could be further modified to apply different transformations to different topics, or when sending to
 * particular consumers.
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
        applyTransformation(response, context);
        return context.forwardResponse(header, response);
    }

    /**
     * Applies the transformation to the response data.
     * @param response the response to be transformed
     * @param context the context
     */
    private void applyTransformation(FetchResponseData response, FilterContext context) {
        response.responses().forEach(responseData -> {
            for (FetchResponseData.PartitionData partitionData : responseData.partitions()) {
                SampleFilterTransformer.transform(partitionData, context, this.config);
            }
        });
    }

}
