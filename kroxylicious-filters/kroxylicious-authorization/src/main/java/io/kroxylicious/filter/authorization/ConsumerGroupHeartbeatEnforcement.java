/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static java.util.function.Function.identity;

public class ConsumerGroupHeartbeatEnforcement extends ApiEnforcement<ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 1;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, ConsumerGroupHeartbeatRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        if (request.subscribedTopicNames() == null || request.subscribedTopicNames().isEmpty()) {
            return context.forwardRequest(header, request);
        }
        else {
            var subscribedTopicSet = new HashSet<>(request.subscribedTopicNames());
            List<Action> actions = subscribedTopicSet.stream().map(topic -> new Action(TopicResource.DESCRIBE, topic)).toList();
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                Map<Decision, List<String>> partitioned = result.partition(subscribedTopicSet, TopicResource.DESCRIBE, identity());
                List<String> denied = partitioned.get(Decision.DENY);
                if (!denied.isEmpty()) {
                    ConsumerGroupHeartbeatResponseData message = new ConsumerGroupHeartbeatResponseData();
                    message.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                    return context.requestFilterResultBuilder().shortCircuitResponse(message).completed();
                }
                return context.forwardRequest(header, request);
            });
        }
    }
}
