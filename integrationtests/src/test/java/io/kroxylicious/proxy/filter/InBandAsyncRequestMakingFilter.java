/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.jetbrains.annotations.NotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;

import junit.framework.AssertionFailedError;

/**
 * A test filter that makes in-band an asynchronous request to the cluster, delaying the forward
 * until it completes.
 */
public class InBandAsyncRequestMakingFilter implements RequestFilter, ResponseFilter {

    private final InBandAsyncRequestMakingFilterConfig config;

    public enum Direction {
        ON_REQUEST,
        ON_RESPONSE;

    }

    public static class InBandAsyncRequestMakingFilterConfig extends BaseConfig {
        /**
         * The API key that will trigger the async request
         */
        private final ApiKeys apiKeyTrigger;

        /**
         * Whether the asynchronous request should be performed on request, on response or both.
         */
        private final Set<Direction> direction;

        /**
         * Target topic to be used by the asynchronous producer.
         */
        private final String topic;

        /**
         * Message for the producer to send
         */
        private final String message;

        @JsonCreator
        public InBandAsyncRequestMakingFilterConfig(@JsonProperty(value = "apiKeyTrigger", required = true) ApiKeys apiKeyTrigger,
                                                    @JsonProperty(value = "direction", required = true) Set<Direction> direction,
                                                    @JsonProperty(value = "topic", required = true) String topic,
                                                    @JsonProperty(value = "message", required = true) String message) {
            this.apiKeyTrigger = apiKeyTrigger;
            this.direction = direction;
            this.topic = topic;
            this.message = message;
        }
    }

    public InBandAsyncRequestMakingFilter(InBandAsyncRequestMakingFilterConfig config) {
        this.config = config;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {

        if (apiKey == config.apiKeyTrigger && config.direction.contains(Direction.ON_REQUEST)) {
            return doProduceRequest(filterContext).thenCompose(u -> filterContext.forwardRequest(header, body));
        }
        else {
            return filterContext.forwardRequest(header, body);
        }

    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (apiKey == config.apiKeyTrigger && config.direction.contains(Direction.ON_RESPONSE)) {
            return doProduceRequest(filterContext).thenCompose(u -> filterContext.forwardResponse(header, body));
        }
        else {
            return filterContext.forwardResponse(header, body);
        }
    }

    private CompletionStage<CompletionStage<Void>> doProduceRequest(KrpcFilterContext filterContext) {
        var produceRequestData = createProduceRequest();

        return filterContext.<ProduceResponseData> sendRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, produceRequestData)
                .thenApply(produceResponseData -> {
                    if (produceResponseData.responses().size() != produceRequestData.topicData().size()) {
                        throw new AssertionFailedError("Unexpected number of produce responses");
                    }

                    var anyError = produceResponseData.responses().stream()
                            .map(TopicProduceResponse::partitionResponses)
                            .flatMap(Collection::stream)
                            .filter(t -> t.errorCode() != Errors.NONE.code())
                            .findAny();

                    if (anyError.isPresent()) {
                        throw new AssertionFailedError("Produce was rejected by server: %s".formatted(produceResponseData));
                    }

                    return null;
                });
    }

    @NotNull
    private ProduceRequestData createProduceRequest() {
        var partitionProduceData = new ProduceRequestData.PartitionProduceData()
                .setIndex(0)
                .setRecords(MemoryRecords.withRecords(CompressionType.NONE,
                        new SimpleRecord("0".getBytes(StandardCharsets.UTF_8), config.message.getBytes(StandardCharsets.UTF_8))));
        var topicProduceData = new ProduceRequestData.TopicProduceData()
                .setName(config.topic)
                .setPartitionData(List.of(partitionProduceData));

        var produceRequestData = new ProduceRequestData()
                .setAcks((short) 1);
        produceRequestData.topicData().add(topicProduceData);
        return produceRequestData;
    }
}
