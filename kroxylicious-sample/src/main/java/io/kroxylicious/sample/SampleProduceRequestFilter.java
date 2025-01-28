/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.sample.config.SampleFilterConfig;
import io.kroxylicious.sample.util.SampleFilterTransformer;

/**
 * A sample ProduceRequestFilter implementation, intended to demonstrate how custom filters work with
 * Kroxylicious.<br />
 * <br />
 * This filter transforms the partition data sent by a Kafka producer in a produce request by replacing all
 * occurrences of the String "foo" with the String "bar". These strings are configurable in the config file,
 * so you could substitute this with any text you want.<br />
 * <br />
 * An example of a use case where this might be applicable is when producers are sending data to Kafka
 * using different formats from what consumers are expecting. You could configure this filter to transform
 * the data sent by producers to Kafka into the format consumers expect. In this example use case, the filter
 * could be further modified to apply different transformations to different topics, or when sent by
 * particular producers.
 */
class SampleProduceRequestFilter implements ProduceRequestFilter {

    private final SampleFilterConfig config;
    private final Timer timer;

    SampleProduceRequestFilter(SampleFilterConfig config) {
        this.config = config;
        this.timer = Timer
                .builder("sample_produce_request_filter_transform")
                .description("Time taken for the SampleProduceRequestFilter to transform the produce data.")
                .tag("filter", "SampleProduceRequestFilter")
                .register(Metrics.globalRegistry);
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
        this.timer.record(() -> applyTransformation(request, context)); // We're timing this to report how long it takes through Micrometer

        return context.forwardRequest(header, request);
    }

    /**
     * Applies the transformation to the request data.
     * @param request the request to be transformed
     * @param context the context
     */
    private void applyTransformation(ProduceRequestData request, FilterContext context) {
        request.topicData().forEach(topicData -> {
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                SampleFilterTransformer.transform(partitionData, context, this.config);
            }
        });
    }

}
