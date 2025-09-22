/*
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class AuthorizationFilter implements RequestFilter, ResponseFilter {

    private static final EnumSet<ApiKeys> requestKeysContainingTopics = EnumSet.of(
            ApiKeys.PRODUCE,
            ApiKeys.FETCH,
            ApiKeys.METADATA);

    private static final EnumSet<ApiKeys> responseKeysContainingTopics = EnumSet.of(
            ApiKeys.PRODUCE,
            ApiKeys.FETCH,
            ApiKeys.METADATA);

    record User(String name) implements Principal {}

    Subject subject(FilterContext context) {
        var user = context.clientSaslContext().map(csc -> new User(csc.authorizationId()))
                .orElse(context.clientTlsContext().flatMap(tc -> tc.clientCertificate())
                        .map(x -> x.getSubjectX500Principal().getName()).map(User::new).orElse(null));
        if (user == null) {
            return new Subject(Set.of());
        }
        else {
            return new Subject(Set.of(user));
        }
    }

    private final Authorizer authorizer;
    private final Map<Integer, Object> buffered;
    private final Map<String, Object> topicCache;

    public AuthorizationFilter(Authorizer authorizer, Map<Integer, Object> buffered, Map<String, Object> topicCache) {
        this.authorizer = authorizer;
        this.buffered = buffered;
        this.topicCache = topicCache;
    }

    static boolean requestContainsTopics(ApiKeys key) {
        return requestKeysContainingTopics.contains(key);
    }

    private boolean isAllTopics(RequestHeaderData header, MetadataRequestData metadataRequest) {
        return (metadataRequest.topics() == null) ||
                (metadataRequest.topics().isEmpty() && header.requestApiVersion() == 0);
    }

    public CompletionStage<RequestFilterResult> onMetadataRequest(ApiKeys apiKey,
                                                                  RequestHeaderData header,
                                                                  MetadataRequestData request,
                                                                  FilterContext context) {
        // A metadata request is idempotent except when topic creation is allowed
        // Therefore it's safe to forward the request as-is
        // (and filter out topics disallowed by the authorizer when handling the response),
        // EXCEPT when the request allows topic creation.
        if (!request.allowAutoTopicCreation()) {
            return context.forwardRequest(header, request);
        }

        // In the topic creation allowed case, we forward the request without topic
        // creation, filter the response with the authorizer
        // and then it again without the disallowed topic creations
        return context.sendRequest(new RequestHeaderData()
                        .setRequestApiVersion(header.requestApiVersion()),
                request.duplicate().setAllowAutoTopicCreation(false))
                .thenCompose(notCreateResponse -> {
                    var notCreateMetadataResponse = (MetadataResponseData) notCreateResponse;
                    var byTopicExistence = notCreateMetadataResponse.topics().stream()
                            .collect(Collectors.partitioningBy(t -> Errors.UNKNOWN_TOPIC_OR_PARTITION.code() == t.errorCode()));
                    var unknownTopics = byTopicExistence.get(true);
                    var knownTopics = byTopicExistence.get(false);
                    var createActions = unknownTopics.stream()
                            .map(t -> new Action(TopicResource.CREATE, t.name()))
                            .toList();
                    Authorization createAuthorization = authorizer.authorize(subject(context), createActions);
                    var createDecisions = request.topics().stream().collect(Collectors.groupingBy(t -> createAuthorization.decision(TopicResource.CREATE, t.name())));
                    var allowedToCreate = createDecisions.get(Decision.ALLOW);
                    if (knownTopics.isEmpty() && allowedToCreate.isEmpty()) {
                        // none of the topics already exists, and the subject is not allowed to create any of them
                        // => in this case we can avoid a second request
                        return context.requestFilterResultBuilder()
                                .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                .completed();
                    }
                    else {
                        var forBuffering = createDecisions.get(Decision.DENY).stream()
                                .map(x -> {
                                    return new MetadataResponseData.MetadataResponseTopic()
                                            .setName(x.name())
                                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                                            //.setTopicAuthorizedOperations()
                                            ;
                                })
                                .toList();
                        buffered.put(header.correlationId(), forBuffering);

                        // We include the topics which we don't need to create,
                        // so that the eventual response is equally fresh for those
                        // as for the topics which are being created
                        var knownTopicNames = knownTopics.stream().map(MetadataResponseData.MetadataResponseTopic::name).collect(Collectors.toSet());
                        var knownTopicIds = knownTopics.stream().map(MetadataResponseData.MetadataResponseTopic::topicId).collect(Collectors.toSet());
                        List<MetadataRequestData.MetadataRequestTopic> concat = Stream.concat(
                                allowedToCreate.stream(),
                                request.topics().stream().filter(rt ->
                                        knownTopicNames.contains(rt.name()) || knownTopicIds.contains(rt.topicId())))
                                        .toList();
                        return context.forwardRequest(header, request.setTopics(concat));
                        // TODO need to add back the create.get(Decision.DENY); in the eventual response
                    }
                });
    }

    public CompletionStage<ResponseFilterResult> onMetadataResponse(ApiKeys apiKey,
                                                                  ResponseHeaderData header,
                                                                  MetadataResponseData response,
                                                                  FilterContext context) {
        List<Action> actionStream = response.topics().stream()
                .map(responseTopic -> new Action(TopicResource.DESCRIBE, responseTopic.name()))
                .toList();
        Authorization authorize = authorizer.authorize(subject(context), actionStream);
        var x = new MetadataResponseData.MetadataResponseTopicCollection(response.topics().size());
        response.topics().stream().map(t -> {
            if (authorize.decision(TopicResource.DESCRIBE, t.name()) == Decision.ALLOW) {
                return t;
            }
            else {
                return new MetadataResponseData.MetadataResponseTopic()
                        .setName(t.name())
                        .setTopicId(t.topicId())
                        .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
            }
        }).forEach(x::add);
        response.setTopics(x);
        return context.forwardResponse(header, response);
    }

    public CompletionStage<RequestFilterResult> onProduceRequest(ApiKeys apiKey,
                                                                  RequestHeaderData header,
                                                                  ProduceRequestData request,
                                                                  FilterContext context) {

        var topicWriteActions = request.topicData().stream()
                .map(t -> new Action(TopicResource.WRITE, t.name()))
                .toList();

        Authorization authorize = authorizer.authorize(subject(context), topicWriteActions);

        var topicWriteDecisions= request.topicData().stream()
                .collect(Collectors.groupingBy(tc -> authorize.decision(TopicResource.WRITE, tc.name())));

        var allowedTopicWrites = topicWriteDecisions.get(Decision.ALLOW);
        if (allowedTopicWrites.isEmpty()) {
            // All denied => short circuit
            return context.requestFilterResultBuilder()
                    .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                    .completed();
        }

        List<ProduceRequestData.TopicProduceData> deniedTopicWrites = topicWriteDecisions.get(Decision.DENY);

        var filteredRequest = request.duplicate();
        var allowed = new ProduceRequestData.TopicProduceDataCollection(allowedTopicWrites.size());
        allowed.addAll(allowedTopicWrites);
        filteredRequest.setTopicData(allowed);

        this.buffered.put(header.correlationId(), deniedTopicWrites.stream().map(topicProduceData -> {
            return new ProduceResponseData.TopicProduceResponse().setName(topicProduceData.name())
                    .setPartitionResponses(topicProduceData.partitionData().stream().map(partitionProduceData ->
                            new ProduceResponseData.PartitionProduceResponse()
                            .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message())
                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                            .toList());
        }).toList());
        return context.forwardRequest(header, filteredRequest);
    }

    private CompletionStage<ResponseFilterResult> onProduceResponse(ApiKeys apiKey,
                                                                    ResponseHeaderData header,
                                                                    ProduceResponseData response,
                                                                    FilterContext context) {
        var o = (List<ProduceResponseData.TopicProduceResponse>) this.buffered.remove(header.correlationId());
        response.responses().addAll(o);
        return context.forwardResponse(header, response);
    }

    private CompletionStage<ResponseFilterResult> onFetchResponse(ApiKeys apiKey,
                                                                  ResponseHeaderData header,
                                                                  FetchResponseData response,
                                                                  FilterContext context) {
        var topicReadActions = response.responses().stream()
                .map(t -> new Action(TopicResource.READ, t.topic()))
                .toList();

        var authorization = authorizer.authorize(subject(context), topicReadActions);

        var topicReadDecisions = response.responses().stream()
                .collect(Collectors.groupingBy(t -> authorization.decision(TopicResource.READ, t.topic())));
        var allowedTopicReads = topicReadDecisions.get(Decision.ALLOW);

        return null;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        return switch (apiKey) {
            case PRODUCE -> onProduceRequest(apiKey, header, (ProduceRequestData) request, context);
            case METADATA -> onMetadataRequest(apiKey, header, (MetadataRequestData) request, context);
            default -> context.forwardRequest(header, request);
        };
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        return switch (apiKey) {
            case PRODUCE -> onProduceResponse(apiKey, header, (ProduceResponseData) response, context);
            case FETCH -> onFetchResponse(apiKey, header, (FetchResponseData) response, context);
            case METADATA -> onMetadataResponse(apiKey, header, (MetadataResponseData) response, context);
            default -> context.forwardResponse(header, response);
        };
    }


}
