/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

import static org.apache.kafka.common.protocol.Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_ID;

class ProduceEnforcement extends ApiEnforcement<ProduceRequestData, ProduceResponseData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceEnforcement.class);

    @Override
    short minSupportedVersion() {
        return 3;
    }

    @Override
    short maxSupportedVersion() {
        return 13;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   ProduceRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Uuid> topicIds = request.topicData().stream()
                .map(ProduceRequestData.TopicProduceData::topicId)
                .filter(uuid -> !Uuid.ZERO_UUID.equals(uuid))
                .toList();
        return context.topicNames(topicIds).thenCompose(topicNameMapping -> {
            boolean requiresResponse = request.acks() != 0;
            if (topicNameMapping.anyFailures()) {
                return topicNameLookupFailure(header, request, context, requiresResponse, topicNameMapping);
            }
            boolean hasTransactionalRecords = RequestDataUtils.hasTransactionalRecords(request);
            String transactionalId = request.transactionalId();
            // fail fast if records flagged as transactional but request has no transactionalId
            if (hasTransactionalRecords && transactionalId == null) {
                return transactionalIdErrorResponse(context, header, request, requiresResponse);
            }
            else {
                var topicWriteActions = request.topicData().stream()
                        .map(t -> new Action(TopicResource.WRITE, getTopicName(t, topicNameMapping)));
                var transactionalIdActions = Optional.ofNullable(transactionalId).stream().map(t -> new Action(TransactionalIdResource.WRITE, t));
                return authorizationFilter.authorization(context, Stream.concat(topicWriteActions, transactionalIdActions).toList()).thenCompose(authorization -> {
                    if (hasTransactionalRecords && authorization.denied(TransactionalIdResource.WRITE).contains(transactionalId)) {
                        return transactionalIdErrorResponse(context, header, request, requiresResponse);
                    }
                    return authorizeTopics(header, request, context, authorizationFilter, authorization, requiresResponse, topicNameMapping);
                });
            }
        });
    }

    private static String getTopicName(ProduceRequestData.TopicProduceData topicData, TopicNameMapping topicNameMapping) {
        return Optional.ofNullable(topicData.name())
                .filter(name -> !name.isEmpty())
                .or(() -> Optional.ofNullable(topicNameMapping.topicNames().get(topicData.topicId())))
                .orElseThrow(() -> new IllegalStateException(
                        String.format("Topic name not found for TopicProduceData{%s, %s}", topicData.name(), topicData.topicId())));
    }

    private static CompletionStage<RequestFilterResult> topicNameLookupFailure(RequestHeaderData header,
                                                                               ProduceRequestData request,
                                                                               FilterContext context,
                                                                               boolean requiresResponse,
                                                                               TopicNameMapping topicNameMapping) {
        if (requiresResponse) {
            return context.requestFilterResultBuilder().errorResponse(header, request, UNKNOWN_TOPIC_ID.exception()).completed();
        }
        else {
            LOGGER.atWarn()
                    .addKeyValue("failures", topicNameMapping::failures)
                    .log("failed to map topic ids to names for acks=0 request, dropping request.");
            return context.requestFilterResultBuilder().drop().completed();
        }
    }

    private static CompletionStage<RequestFilterResult> authorizeTopics(RequestHeaderData header,
                                                                        ProduceRequestData request,
                                                                        FilterContext context,
                                                                        AuthorizationFilter authorizationFilter,
                                                                        AuthorizeResult authorization,
                                                                        boolean requiresResponse, TopicNameMapping topicNameMapping) {
        var topicWriteDecisions = authorization.partition(request.topicData(),
                TopicResource.WRITE, topicData -> getTopicName(topicData, topicNameMapping));

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
                        .setName(getTopicName(topicProduceData, topicNameMapping))
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

    private static CompletionStage<RequestFilterResult> transactionalIdErrorResponse(FilterContext context, RequestHeaderData header, ProduceRequestData produceRequest,
                                                                                     boolean requiresResponse) {
        if (requiresResponse) {
            return context.requestFilterResultBuilder().errorResponse(header, produceRequest, TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception()).completed();
        }
        else {
            return context.requestFilterResultBuilder().drop().completed();
        }
    }

}
