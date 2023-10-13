/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginConfigType;

@PluginConfigType(RejectingCreateTopicFilterFactory.Config.class)
public class RejectingCreateTopicFilterFactory implements FilterFactory<RejectingCreateTopicFilterFactory.Config, RejectingCreateTopicFilterFactory.Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        // null configuration is allowed, by default null config is invalid
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config configuration) {
        return new Filter(context, configuration);
    }

    /**
     * A test filter that rejects all create topic requests with a short-circuit
     * error response.  The request never reaches the broker.
     */
    public static class Filter implements CreateTopicsRequestFilter {

        public static final String ERROR_MESSAGE = "rejecting all topics";
        private final ForwardingStyle forwardingStyle;
        private final boolean withCloseConnection;
        private final FilterFactoryContext constructionContext;

        public Filter(FilterFactoryContext constructionContext, Config config) {
            this.constructionContext = constructionContext;
            config = config == null ? new Config(false, ForwardingStyle.SYNCHRONOUS) : config;
            this.withCloseConnection = config.withCloseConnection;
            this.forwardingStyle = config.forwardingStyle;
        }

        @Override
        public CompletionStage<RequestFilterResult> onCreateTopicsRequest(short apiVersion, RequestHeaderData header, CreateTopicsRequestData request,
                                                                          FilterContext context) {
            return forwardingStyle.apply(new ForwardingContext(context, constructionContext, request))
                    .thenCompose((u) -> {
                        CreateTopicsResponseData response = new CreateTopicsResponseData();
                        CreateTopicsResponseData.CreatableTopicResultCollection topics = new CreateTopicsResponseData.CreatableTopicResultCollection();
                        allocateByteBufToTestKroxyliciousReleasesIt(context);
                        request.topics().forEach(creatableTopic -> {
                            CreateTopicsResponseData.CreatableTopicResult result = new CreateTopicsResponseData.CreatableTopicResult();
                            result.setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code()).setErrorMessage(ERROR_MESSAGE);
                            result.setName(creatableTopic.name());
                            topics.add(result);
                        });
                        response.setTopics(topics);

                        var builder = context.requestFilterResultBuilder().shortCircuitResponse(response);
                        if (withCloseConnection) {
                            builder.withCloseConnection();
                        }
                        return builder.completed();
                    });
        }

        private static void allocateByteBufToTestKroxyliciousReleasesIt(FilterContext context) {
            context.createByteBufferOutputStream(4000);
        }

    }

    public static class Config {

        /*
         * If true, rejection will also close the connection
         */
        private final boolean withCloseConnection;
        private final ForwardingStyle forwardingStyle;

        @JsonCreator
        public Config(@JsonProperty(value = "withCloseConnection") boolean withCloseConnection,
                      @JsonProperty(value = "forwardingStyle") ForwardingStyle forwardingStyle) {
            this.withCloseConnection = withCloseConnection;
            this.forwardingStyle = forwardingStyle == null ? ForwardingStyle.SYNCHRONOUS : forwardingStyle;
        }
    }
}
