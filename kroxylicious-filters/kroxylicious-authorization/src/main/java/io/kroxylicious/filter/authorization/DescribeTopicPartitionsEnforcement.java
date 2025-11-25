/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

class DescribeTopicPartitionsEnforcement extends ApiEnforcement<DescribeTopicPartitionsRequestData, DescribeTopicPartitionsResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 0;
    }

    private static final AllTopicsInflightState ALL_TOPIC_INFLIGHT_STATE = new AllTopicsInflightState();

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, DescribeTopicPartitionsRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        if (request.topics().isEmpty()) {
            authorizationFilter.pushInflightState(header, ALL_TOPIC_INFLIGHT_STATE);
            return context.forwardRequest(header, request);
        }
        else {
            // resolve all TopicResource operations for the purposes of constructing authorized operations
            List<Action> actions = request.topics().stream().flatMap(topicRequest -> Arrays.stream(TopicResource.values()).map(r -> new Action(r, topicRequest.name())))
                    .toList();
            return authorizationFilter.authorization(context, actions).thenCompose(authorizeResult -> {
                Map<Decision, List<DescribeTopicPartitionsRequestData.TopicRequest>> partitioned = authorizeResult.partition(request.topics(), TopicResource.DESCRIBE,
                        DescribeTopicPartitionsRequestData.TopicRequest::name);
                var unauthorized = partitioned.get(Decision.DENY).stream().map(topic -> {
                    DescribeTopicPartitionsResponseTopic responseTopic = new DescribeTopicPartitionsResponseTopic();
                    responseTopic.setName(topic.name());
                    responseTopic.setTopicId(Uuid.ZERO_UUID);
                    responseTopic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                    responseTopic.setIsInternal(false);
                    responseTopic.setPartitions(List.of());
                    return responseTopic;
                }).toList();
                if (authorizeResult.allowed().isEmpty()) {
                    var response = new DescribeTopicPartitionsResponseData();
                    response.topics().addAll(unauthorized);
                    return context.requestFilterResultBuilder().shortCircuitResponse(response).completed();
                }
                else {
                    // for this RPC we can not short circuit just because there are no denied topics because we need to
                    // populate the topic authorized operations
                    request.topics().removeAll(partitioned.get(Decision.DENY));
                    authorizationFilter.pushInflightState(header, (DescribeTopicPartitionsResponseData response) -> {
                        // following the real broker behaviour we do not need to compute this for unauthorized topics
                        for (DescribeTopicPartitionsResponseTopic topic : response.topics()) {
                            topic.setTopicAuthorizedOperations(AuthorizedOps.topicAuthorizedOps(authorizeResult, topic.topicAuthorizedOperations(), topic.name()));
                        }
                        response.topics().addAll(unauthorized);
                        return response;
                    });
                    return context.forwardRequest(header, request);
                }
            });
        }
    }

    record AllTopicsInflightState() implements InflightState<DescribeTopicPartitionsResponseData> {

        @Override
        public DescribeTopicPartitionsResponseData merge(DescribeTopicPartitionsResponseData describeTopicPartitionsResponseData) {
            throw new IllegalStateException("synchronous merge not possible for all-topics, we must filter out denied topics");
        }
    }

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header, DescribeTopicPartitionsResponseData response, FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        InflightState<DescribeTopicPartitionsResponseData> inflightState = authorizationFilter.popInflightState(header, InflightState.class);
        if (inflightState == null) {
            return context.forwardResponse(header, response);
        }
        else if (inflightState instanceof AllTopicsInflightState) {
            return removeUnauthorizedTopics(header, response, context, authorizationFilter);
        }
        else {
            return context.forwardResponse(header, inflightState.merge(response));
        }
    }

    private static CompletionStage<ResponseFilterResult> removeUnauthorizedTopics(ResponseHeaderData header, DescribeTopicPartitionsResponseData response,
                                                                                  FilterContext context, AuthorizationFilter authorizationFilter) {
        // resolve all TopicResource operations for the purposes of constructing authorized operations
        List<Action> actions = response.topics().stream().flatMap(topicRequest -> Arrays.stream(TopicResource.values()).map(r -> new Action(r, topicRequest.name())))
                .toList();
        CompletionStage<AuthorizeResult> stage = authorizationFilter.authorization(context, actions);
        return stage.thenCompose(authorizeResult -> {
            Map<Decision, List<DescribeTopicPartitionsResponseTopic>> partitioned = authorizeResult.partition(response.topics(), TopicResource.DESCRIBE,
                    DescribeTopicPartitionsResponseTopic::name);
            response.topics().removeAll(partitioned.get(Decision.DENY));
            for (DescribeTopicPartitionsResponseTopic topic : response.topics()) {
                topic.setTopicAuthorizedOperations(AuthorizedOps.topicAuthorizedOps(authorizeResult, topic.topicAuthorizedOperations(), topic.name()));
            }
            return context.forwardResponse(header, response);
        });
    }

}
