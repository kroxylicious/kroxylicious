/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestUtils;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static org.apache.kafka.common.protocol.Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED;

class ProduceEnforcement extends ApiEnforcement<ProduceRequestData, ProduceResponseData> {
    @Override
    short minSupportedVersion() {
        return 3;
    }

    @Override
    short maxSupportedVersion() {
        return 12;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   ProduceRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        ProduceRequest produceRequest = new ProduceRequest(request, header.requestApiVersion());
        boolean requiresResponse = request.acks() != 0;
        boolean hasTransactionalRecords = RequestUtils.hasTransactionalRecords(produceRequest);
        String transactionalId = request.transactionalId();
        // fail fast if records flagged as transactional but request has no transactionalId
        if (hasTransactionalRecords && transactionalId == null) {
            return transactionalIdErrorResponse(context, produceRequest, requiresResponse);
        }
        else {
            var topicWriteActions = request.topicData().stream()
                    .map(t -> new Action(TopicResource.WRITE, t.name()));
            var transactionalIdActions = hasTransactionalRecords ? Stream.of(new Action(TransactionalIdResource.WRITE, transactionalId)) : Stream.<Action> empty();
            return authorizationFilter.authorization(context, Stream.concat(topicWriteActions, transactionalIdActions).toList()).thenCompose(authorization -> {
                if (hasTransactionalRecords && authorization.denied(TransactionalIdResource.WRITE).contains(transactionalId)) {
                    return transactionalIdErrorResponse(context, produceRequest, requiresResponse);
                }
                return authorizeTopics(header, request, context, authorizationFilter, authorization, requiresResponse);
            });
        }
    }

    private static CompletionStage<RequestFilterResult> authorizeTopics(RequestHeaderData header, ProduceRequestData request, FilterContext context,
                                                                        AuthorizationFilter authorizationFilter, AuthorizeResult authorization,
                                                                        boolean requiresResponse) {
        var topicWriteDecisions = authorization.partition(request.topicData(),
                TopicResource.WRITE, ProduceRequestData.TopicProduceData::name);

        var allowedTopicWrites = topicWriteDecisions.get(Decision.ALLOW);
        if (allowedTopicWrites.isEmpty()) {
            if (requiresResponse) {
                return context.requestFilterResultBuilder()
                        .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                        .completed();
            }
            else {
                return context.requestFilterResultBuilder().drop().completed();
            }
        }

        var deniedTopicWrites = topicWriteDecisions.get(Decision.DENY);

        if (deniedTopicWrites.isEmpty()) {
            // nothing denied, forward whole request on
            return context.forwardRequest(header, request);
        }

        for (var topic : deniedTopicWrites) {
            request.topicData().remove(topic);
        }

        var topicProduceResponses = deniedTopicWrites.stream()
                .map(topicProduceData -> new ProduceResponseData.TopicProduceResponse()
                        .setName(topicProduceData.name())
                        .setPartitionResponses(topicProduceData.partitionData().stream()
                                .map(partitionProduceData -> new ProduceResponseData.PartitionProduceResponse()
                                        .setIndex(partitionProduceData.index())
                                        .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message())
                                        .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                                .toList()))
                .toList();

        if (requiresResponse) {
            authorizationFilter.pushInflightState(header, (ProduceResponseData response) -> {
                response.responses().addAll(topicProduceResponses);
                return response;
            });
        }
        return context.forwardRequest(header, request);
    }

    private static CompletionStage<RequestFilterResult> transactionalIdErrorResponse(FilterContext context, ProduceRequest produceRequest, boolean requiresResponse) {
        if (requiresResponse) {
            ApiMessage response = produceRequest.getErrorResponse(TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception()).data();
            return context.requestFilterResultBuilder().shortCircuitResponse(response).completed();
        }
        else {
            return context.requestFilterResultBuilder().drop().completed();
        }
    }

}
