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

/**
 * A test filter that rejects all create topic requests with a short-circuit
 * error response.  The request never reaches the broker.
 */
public class RejectingCreateTopicFilter implements CreateTopicsRequestFilter {

    public static final String ERROR_MESSAGE = "rejecting all topics";
    private final ForwardingStyle forwardingStyle;
    private final boolean withCloseConnection;
    private final FilterFactoryContext constructionContext;

    public RejectingCreateTopicFilter(FilterFactoryContext constructionContext, RejectingCreateTopicFilterConfig config) {
        this.constructionContext = constructionContext;
        config = config == null ? new RejectingCreateTopicFilterConfig(false, ForwardingStyle.SYNCHRONOUS) : config;
        this.withCloseConnection = config.withCloseConnection();
        this.forwardingStyle = config.forwardingStyle();
    }

    @Override
    public CompletionStage<RequestFilterResult> onCreateTopicsRequest(
            short apiVersion,
            RequestHeaderData header,
            CreateTopicsRequestData request,
            FilterContext context
    ) {
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

    /**
     * @param withCloseConnection
     * If true, rejection will also close the connection */
    public record RejectingCreateTopicFilterConfig(boolean withCloseConnection, ForwardingStyle forwardingStyle) {

        @JsonCreator
        public RejectingCreateTopicFilterConfig(
                @JsonProperty(value = "withCloseConnection")
                boolean withCloseConnection,
                @JsonProperty(value = "forwardingStyle")
                ForwardingStyle forwardingStyle
        ) {
            this.withCloseConnection = withCloseConnection;
            this.forwardingStyle = forwardingStyle == null ? ForwardingStyle.SYNCHRONOUS : forwardingStyle;
        }
    }
}
