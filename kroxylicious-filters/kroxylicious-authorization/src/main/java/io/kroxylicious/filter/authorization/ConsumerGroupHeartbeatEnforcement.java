/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static java.util.function.Function.identity;

class ConsumerGroupHeartbeatEnforcement extends ApiEnforcement<ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 1;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   ConsumerGroupHeartbeatRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        Action groupReadAction = new Action(GroupResource.READ, request.groupId());
        List<Action> actions = Stream.concat(Stream.of(groupReadAction), topicDescribeActions(request)).toList();
        return authorizationFilter.authorization(context, actions).thenCompose(result -> {
            if (result.denied().contains(groupReadAction)) {
                return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED.exception()).completed();
            }
            else if (request.subscribedTopicNames() == null || request.subscribedTopicNames().isEmpty()) {
                return context.forwardRequest(header, request);
            }
            else {
                Map<Decision, List<String>> partitioned = result.partition(request.subscribedTopicNames(), TopicResource.DESCRIBE, identity());
                List<String> denied = partitioned.get(Decision.DENY);
                if (!denied.isEmpty()) {
                    ConsumerGroupHeartbeatResponseData message = new ConsumerGroupHeartbeatResponseData();
                    message.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                    // Note the broker does not set an error message for this error code, and we copy that behaviour.
                    return context.requestFilterResultBuilder().shortCircuitResponse(message).completed();
                }
                return context.forwardRequest(header, request);
            }
        });
    }

    private static Stream<Action> topicDescribeActions(ConsumerGroupHeartbeatRequestData request) {
        return Optional.ofNullable(request.subscribedTopicNames()).stream()
                .flatMap(Collection::stream)
                .map(topic -> new Action(TopicResource.DESCRIBE, topic));
    }
}
