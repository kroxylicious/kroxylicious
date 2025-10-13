/*
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Authorization;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;

public class AuthorizationFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(io.kroxylicious.filter.authorization.Authorization.class);

    private final Authorizer authorizer;
    private final Map<Integer, UnaryOperator<?>> bufferedPartialResponses;
    private boolean includeClusterAuthorizedOperations; // TODO should be keys on correlation id
    private boolean includeTopicAuthorizedOperations; // TODO should be keys on correlation id
    private boolean isAllTopics;
    private boolean requestUsesTopicIds;
    private short useMetadataVersion = -1;

    public AuthorizationFilter(Authorizer authorizer) {
        this.authorizer = authorizer;
        this.bufferedPartialResponses = new HashMap<>(10);
    }

    CompletionStage<Subject> subject(FilterContext context) {
        return context.authenticatedSubject();
    }

    @NonNull
    private CompletionStage<Authorization> authorization(FilterContext context, List<Action> actions) {
        return subject(context)
                .thenCompose(subject -> authorizer.authorize(subject, actions))
                .thenApply(authz -> {
                    LOG.info("DENY {} to {}", authz.denied(), authz.subject());
                    return authz;
                });
    }

    private boolean isAllTopics(RequestHeaderData header, MetadataRequestData metadataRequest) {
        return (metadataRequest.topics() == null) ||
                (metadataRequest.topics().isEmpty() && header.requestApiVersion() == 0);
    }

    <R> void savePartialResponse(RequestHeaderData header, UnaryOperator<R> applier) {
        var existing = bufferedPartialResponses.put(header.correlationId(), applier);
        if (existing != null) {
            throw new IllegalStateException("Already have partial response");
        }
    }

    <R> R mergePartialResponse(ResponseHeaderData header, R response) {
        var partialResponse = this.bufferedPartialResponses.remove(header.correlationId());
        if (partialResponse != null) {
            return (R) ((UnaryOperator) partialResponse).apply((Object) response);
        }
        else {
            return response;
        }
    }

    public CompletionStage<RequestFilterResult> onMetadataRequest(RequestHeaderData header,
                                                                  MetadataRequestData request,
                                                                  FilterContext context) {
        this.includeClusterAuthorizedOperations = header.requestApiVersion() >= 8
                && header.requestApiVersion() <= 10
                && request.includeClusterAuthorizedOperations();
        this.includeTopicAuthorizedOperations = request.includeTopicAuthorizedOperations();
        this.isAllTopics = isAllTopics(header, request);
        this.requestUsesTopicIds = request.topics() != null
                && !request.topics().isEmpty()
                && request.topics().get(0).topicId() != null
                && !request.topics().get(0).topicId().equals(Uuid.ZERO_UUID);

        // A metadata request is idempotent EXCEPT when topic creation is allowed.
        // Therefore, it's safe to forward the requests with allowAutoTopicCreation=false as-is
        // (and leave the response handler to filter out topics disallowed by the authorizer),
        // EXCEPT when the request allows topic creation.
        if (!this.requestUsesTopicIds &&
                (this.isAllTopics // An all-topics query won't create topics even if the flag is set
                || !request.allowAutoTopicCreation())) {
            return context.forwardRequest(header, request);
        }

        return onNonIdempotentMetadataRequest(header, request, context);
    }

    private CompletionStage<RequestFilterResult> onNonIdempotentMetadataRequest(RequestHeaderData header,
                                                                                MetadataRequestData request,
                                                                                FilterContext context) {
        // Problem: If we forward a allowAutoTopicCreation=true request without filtering we could create
        // topics which the user is not allowed to create. But if we filter them out, and they do exist,
        // then we can't return their metadata.
        // Solution: We forward the request with allowAutoTopicCreation=false, filter the response with
        // our authorizer and then forward it again without the _disallowed_ topic creations.
        // Merge the two responses and return to the client.
        // Subtlety: V4 added the flag, before that all requests are allowAutoTopicCreation=true
        // Solution: Always use a version >= 4 for the initial request, and revert to the client's chosen
        // version for the subsequent request.

        var initialRequestHeader = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.METADATA.id)
                .setRequestApiVersion(useMetadataVersion != -1 ? useMetadataVersion : ApiKeys.METADATA.latestVersion()) // Should support topic ids
                .setClientId(header.clientId());
        var initialRequest = new MetadataRequestData()
                .setTopics(request.topics())
                .setAllowAutoTopicCreation(false);
        return context.sendRequest(initialRequestHeader, initialRequest)
                .thenCompose(notCreateResponse -> {
                    var notCreateMetadataResponse = (MetadataResponseData) notCreateResponse;
                    var responseTopicsByExistence = notCreateMetadataResponse.topics().stream()
                            .collect(Collectors.partitioningBy(responseTopic ->
                                    Errors.UNKNOWN_TOPIC_OR_PARTITION.code() == responseTopic.errorCode()));
                    var notExistingTopics = responseTopicsByExistence.get(true);
                    var alreadyExistingTopics = responseTopicsByExistence.get(false);
                    var createAndDescribeActions = notExistingTopics.stream()
                            .flatMap(responseTopic -> Stream.of(
                                    new Action(TopicResource.CREATE, responseTopic.name()),
                                    new Action(TopicResource.DESCRIBE, responseTopic.name())))
                            .toList();
                    return authorization(context, createAndDescribeActions)
                            .thenCompose(createAndDescribeAuthorization -> {
                                var createDecisions = notExistingTopics.stream()
                                        .flatMap(responseTopic -> request.topics().stream().filter(x -> Objects.equals(responseTopic.name(), x.name())).findFirst().stream())
                                        .collect(Collectors.groupingBy(responseTopic -> createAndDescribeAuthorization.decision(TopicResource.CREATE, responseTopic.name())));
                                var notExistingAndAllowedToCreate = createDecisions.getOrDefault(Decision.ALLOW, List.of());
                                var notExistingAndNotAllowedToCreate = createDecisions.getOrDefault(Decision.DENY, List.of());
                                var forBuffering = notExistingAndNotAllowedToCreate.stream()
                                    .map(
                                        requestTopic -> {
                                            var decision = createAndDescribeAuthorization.decision(TopicResource.DESCRIBE, requestTopic.name());
                                            return new MetadataResponseData.MetadataResponseTopic()
                                                    .setName(requestTopic.name())
                                                    .setErrorCode((decision == Decision.ALLOW ? Errors.UNKNOWN_TOPIC_OR_PARTITION : Errors.TOPIC_AUTHORIZATION_FAILED).code());
                                        })
                                    .toList();
                                savePartialResponse(header, (MetadataResponseData response) -> {
                                    response.topics().addAll(forBuffering);
                                    return response;
                                });

                                // We include the topics which we don't need to create,
                                // so that the eventual response is equally fresh for those
                                // as for the topics which are being created
                                var knownTopicNames = alreadyExistingTopics.stream()
                                        .map(MetadataResponseData.MetadataResponseTopic::name)
                                        .collect(Collectors.toSet());
                                var knownTopicIds = alreadyExistingTopics.stream()
                                        .map(MetadataResponseData.MetadataResponseTopic::topicId)
                                        .collect(Collectors.toSet());
                                var concat = notExistingAndAllowedToCreate.stream();
                                if (request.topics() != null) {
                                    concat = Stream.concat(concat,
                                            request.topics().stream()
                                                    .filter(requestTopic ->
                                                            this.requestUsesTopicIds ? knownTopicIds.contains(requestTopic.topicId()) : knownTopicNames.contains(requestTopic.name())));
                                }

                                // We can now forward the client's original request, albeit with a filtered list of topics
                                var requestWithCreateTopics = concat.toList();
                                return context.forwardRequest(header, request.setTopics(requestWithCreateTopics));
                    });
                });
    }

    public CompletionStage<ResponseFilterResult> onMetadataResponse(ResponseHeaderData header,
                                                                    MetadataResponseData response,
                                                                    FilterContext context) {
        List<Action> actions = new ArrayList<>();
        if (includeClusterAuthorizedOperations) {
             for (var clusterOp : ClusterResource.values()) {
                 actions.add(new Action(clusterOp, ""));
             }
        }
        if (includeTopicAuthorizedOperations) {
            actions.addAll(response.topics().stream()
                    .map(MetadataResponseData.MetadataResponseTopic::name)
                    .filter(Objects::nonNull)
                    .flatMap(topicName -> Arrays.stream(TopicResource.values()).map(op -> new Action(op, topicName)))
                    .toList());
        }
        else {
            actions.addAll(response.topics().stream()
                    .map(MetadataResponseData.MetadataResponseTopic::name)
                    .filter(Objects::nonNull)
                    .map(topicName -> new Action(TopicResource.DESCRIBE, topicName))
                    .toList());
        }

        return authorization(context, actions)
                .thenCompose(authorize -> {
                    var toRemove = new ArrayList<MetadataResponseData.MetadataResponseTopic>();

                    for (var t : response.topics()) {
                        if (authorize.decision(TopicResource.DESCRIBE, t.name()) == Decision.DENY) {
                            if (isAllTopics) {
                                toRemove.add(t);
                            }
                            else {
                                // TODO in this case do we return the topic Id if the client didn't already know it?
                                t.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                                t.partitions().clear();
                                t.setIsInternal(false);
                                t.setTopicAuthorizedOperations(Integer.MIN_VALUE);
                                if (this.requestUsesTopicIds) {
                                    t.setName(null);
                                }
                                else {
                                    t.setTopicId(Uuid.ZERO_UUID);
                                }
                            }
                        }
                        else { // ALLOW
                            if (includeTopicAuthorizedOperations) {
                                t.setTopicAuthorizedOperations(topicAuthzOptions(authorize, t));
                            }
                        }
                    }

                    if (includeClusterAuthorizedOperations) {
                        response.setClusterAuthorizedOperations(clusterAuthzOptions(authorize));
                    }
                    response.topics().removeAll(toRemove);
                    mergePartialResponse(header, response);

                    return context.forwardResponse(header, response);
                });
    }

    private static int clusterAuthzOptions(Authorization authorize) {
        int flag = 0;
        for (var clusterOp : ClusterResource.values()) {
            if (authorize.decision(clusterOp, "") == Decision.ALLOW) {
                flag |= (0x1 << clusterOp.kafkaOrdinal);
            }
        }
        return flag;
    }

    private static int topicAuthzOptions(Authorization authorize, MetadataResponseData.MetadataResponseTopic t) {
        int flag = 0;
        for (var clusterOp : TopicResource.values()) {
            if (authorize.decision(clusterOp, t.name()) == Decision.ALLOW) {
                flag |= (0x1 << clusterOp.kafkaOrdinal);
            }
        }
        return flag;
    }

    public CompletionStage<RequestFilterResult> onProduceRequest(RequestHeaderData header,
                                                                 ProduceRequestData request,
                                                                 FilterContext context) {
        var topicWriteActions = request.topicData().stream()
                .map(t -> new Action(TopicResource.WRITE, t.name()))
                .toList();

        return authorization(context, topicWriteActions).thenCompose(authorization -> {

            var topicWriteDecisions = authorization.partition(request.topicData(),
                    TopicResource.WRITE, ProduceRequestData.TopicProduceData::name);

//            var topicWriteDecisions = request.topicData().stream()
//                    .collect(Collectors.groupingBy(tc -> authorization.decision(TopicResource.WRITE, tc.name())));

            var allowedTopicWrites = topicWriteDecisions.get(Decision.ALLOW);
            if (allowedTopicWrites.isEmpty()) {
                // All denied => short circuit
                return context.requestFilterResultBuilder()
                        .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                        .completed();
            }

            var deniedTopicWrites = topicWriteDecisions.get(Decision.DENY);

            for (var topic : deniedTopicWrites) {
                request.topicData().remove(topic);
            }

            var topicProduceResponses = deniedTopicWrites.stream().map(topicProduceData -> {
                return new ProduceResponseData.TopicProduceResponse().setName(topicProduceData.name())
                        .setPartitionResponses(topicProduceData.partitionData().stream().map(partitionProduceData -> new ProduceResponseData.PartitionProduceResponse()
                                        .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message())
                                        .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                                .toList());
            }).toList();

            savePartialResponse(header, (ProduceResponseData response) -> {
                response.responses().addAll(topicProduceResponses);
                return response;
            });
            return context.forwardRequest(header, request);
        });
    }

    private CompletionStage<ResponseFilterResult> onProduceResponse(ResponseHeaderData header,
                                                                    ProduceResponseData response,
                                                                    FilterContext context) {
        return context.forwardResponse(header, mergePartialResponse(header, response));
    }

    private CompletionStage<RequestFilterResult> onFetchRequest(RequestHeaderData header,
                                                                FetchRequestData request,
                                                                FilterContext context) {
        var topicReadActions = request.topics().stream()
                .map(t -> new Action(TopicResource.READ, t.topic()))
                .toList();
        return authorization(context, topicReadActions)
                .thenCompose(authorization -> {
                    var topicReadDecisions = request.topics().stream()
                            .collect(Collectors.groupingBy(t -> authorization.decision(TopicResource.READ, t.topic())));
                    if (topicReadDecisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there's no allowed topics
                        return context.requestFilterResultBuilder()
                                .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                .completed();
                    }
                    else if (topicReadDecisions.get(Decision.DENY).isEmpty()) {
                        // Just forward if there's no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        request.setTopics(topicReadDecisions.get(Decision.ALLOW));
                        var fetchableTopicResponses = topicReadDecisions.get(Decision.DENY)
                                .stream().map(t -> new FetchResponseData.FetchableTopicResponse()
                                        .setTopic(t.topic())
                                        .setTopicId(t.topicId())
                                        .setPartitions(t.partitions().stream().map(p -> new FetchResponseData.PartitionData()
                                                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())).toList()))
                                .toList();
                        savePartialResponse(header, (FetchResponseData response) -> {
                            response.responses().addAll(fetchableTopicResponses);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    private CompletionStage<ResponseFilterResult> onFetchResponse(ResponseHeaderData header,
                                                                  FetchResponseData response,
                                                                  FilterContext context) {
        return context.forwardResponse(header, mergePartialResponse(header, response));
    }

    /**
     * Buffer responses for topics where the subject lacks CREATE
     */
    private CompletionStage<RequestFilterResult> onCreateTopicsRequest(RequestHeaderData header,
                                                                       CreateTopicsRequestData request,
                                                                       FilterContext context) {
        var topicReadActions = TopicResource.CREATE.actionsOf(
                request.topics().stream()
                        .map(CreateTopicsRequestData.CreatableTopic::name));
        return authorization(context, topicReadActions)
                .thenCompose(authorization -> {
                    var decisions = authorization.partition(request.topics(),
                            TopicResource.CREATE,
                            CreateTopicsRequestData.CreatableTopic::name);
                    if (decisions.get(Decision.ALLOW).isEmpty()) {
                        // Shortcircuit if there's no allowed topics
                        CreateTopicsResponseData.CreatableTopicResultCollection creatableTopics = new CreateTopicsResponseData.CreatableTopicResultCollection();
                        decisions.get(Decision.DENY).stream()
                                .map(ct -> getCreatableTopicResult(header, ct))
                                .forEach(creatableTopics::add);
                        return context.requestFilterResultBuilder().shortCircuitResponse(
                                new ResponseHeaderData().setCorrelationId(header.correlationId()),
                                new CreateTopicsResponseData().setTopics(creatableTopics)).completed();
//                        return context.requestFilterResultBuilder()
//                                .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
//                                .completed();
                    }
                    else if (decisions.get(Decision.DENY).isEmpty()) {
                        // Just forward if there's no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        var xx = new CreateTopicsRequestData.CreatableTopicCollection();
                        for (var yy : decisions.get(Decision.ALLOW)) {
                            xx.mustAdd(yy.duplicate());
                        }
                        request.setTopics(xx);
                        var creatableTopicResults = decisions.get(Decision.DENY)
                                .stream().map(t -> getCreatableTopicResult(header, t))
                                .toList();
                        savePartialResponse(header, (CreateTopicsResponseData response) -> {
                            response.topics().addAll(creatableTopicResults);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    private static CreateTopicsResponseData.CreatableTopicResult getCreatableTopicResult(RequestHeaderData header,
                                                                                         CreateTopicsRequestData.CreatableTopic t) {
        return new CreateTopicsResponseData.CreatableTopicResult()
                .setName(t.name())
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                .setErrorMessage(header.requestApiVersion() >= 1 ? "Authorization failed." : null);
    }

    /**
     * Filter out any topic configs if the subject lacks DESCRIBE_CONFIGS
     * Append responses buffered when checking the request
     */
    private CompletionStage<ResponseFilterResult> onCreateTopicsResponse(ResponseHeaderData header,
                                                                         CreateTopicsResponseData response,
                                                                         FilterContext context) {

        List<Action> actions = TopicResource.DESCRIBE_CONFIGS.actionsOf(response.topics().stream().map(t -> t.name()));
        return authorization(context, actions)
                .thenCompose(authorization -> {

                    for (var creatableTopicResult : response.topics()) {
                        if (authorization.decision(TopicResource.DESCRIBE_CONFIGS, creatableTopicResult.name()) == Decision.DENY) {
                            creatableTopicResult.setConfigs(List.of());
                            creatableTopicResult.setReplicationFactor((short) -1);
                            creatableTopicResult.setNumPartitions(-1);
                            creatableTopicResult.setTopicConfigErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                        }
                    }
                    return context.forwardResponse(header, mergePartialResponse(header, response));
                });
    }

    /**
     * Buffer responses for topics where the subject lacks DELETE
     */
    private CompletionStage<RequestFilterResult> onDeleteTopicsRequest(RequestHeaderData header,
                                                                       DeleteTopicsRequestData request,
                                                                       FilterContext context) {
        short apiVersion = header.requestApiVersion();
        boolean useStates = apiVersion >= 6;
        List<Action> actions;
        TopicResource operation = TopicResource.DELETE;
        if (useStates) {
            actions = operation.actionsOf(request.topics().stream()
                    .map(t -> {
                        if (t.name() != null) {
                            return t.name();
                        }
                        else {
                            throw topicIdsNotSupported();
                        }
                    }));
        }
        else {
            actions = operation.actionsOf(request.topicNames().stream());
        }
        return authorization(context, actions)
                .thenCompose(authorization -> {
                    if (useStates) {
                        var decisions = authorization.partition(request.topics(), operation, DeleteTopicsRequestData.DeleteTopicState::name);
                        if (decisions.get(Decision.ALLOW).isEmpty()) {
                            // Shortcircuit if there's no allowed actions
                            return context.requestFilterResultBuilder()
                                    .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                    .completed();
                        }
                        else if (decisions.get(Decision.DENY).isEmpty()) {
                            // Just forward if there's no denied actions
                            return context.forwardRequest(header, request);
                        }
                        else {
                            request.setTopicNames(request.topicNames().stream()
                                    .filter(tn -> authorization.decision(operation, tn) == Decision.ALLOW)
                                    .toList());
                            request.setTopics(request.topics().stream()
                                    .filter(topicState -> authorization.decision(operation, topicState.name()) == Decision.ALLOW)
                                    .toList());

                            var list = decisions.get(Decision.DENY)
                                    .stream().map(t -> new DeleteTopicsResponseData.DeletableTopicResult()
                                            .setName(t.name())
                                            .setTopicId(Uuid.ZERO_UUID) // TODO topic Ids
                                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                                            .setErrorMessage(apiVersion >= 5 ? Errors.TOPIC_AUTHORIZATION_FAILED.message() : null))
                                    .toList();
                            savePartialResponse(header, (DeleteTopicsResponseData response) -> {
                                response.responses().addAll(list);
                                return response;
                            });
                            return context.forwardRequest(header, request);
                        }
                    }
                    else {
                        var decisions = authorization.partition(request.topicNames(), operation, Function.identity());
                        if (decisions.get(Decision.ALLOW).isEmpty()) {
                            // Shortcircuit if there's no allowed actions
                            return context.requestFilterResultBuilder()
                                    .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                    .completed();
                        }
                        else if (decisions.get(Decision.DENY).isEmpty()) {
                            // Just forward if there's no denied actions
                            return context.forwardRequest(header, request);
                        }
                        else {
                            request.setTopicNames(request.topicNames().stream()
                                    .filter(tn -> authorization.decision(operation, tn) == Decision.ALLOW)
                                    .toList());

                            var list = decisions.get(Decision.DENY)
                                    .stream().map(t -> new DeleteTopicsResponseData.DeletableTopicResult()
                                            .setName(t)
                                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                                            .setErrorMessage(apiVersion >= 5 ? Errors.TOPIC_AUTHORIZATION_FAILED.message() : null))
                                    .toList();
                            savePartialResponse(header, (DeleteTopicsResponseData response) -> {
                                response.responses().addAll(list);
                                return response;
                            });
                            return context.forwardRequest(header, request);
                        }
                    }
                });
    }

    /**
     * Append responses buffered when checking the request
     */
    private CompletionStage<ResponseFilterResult> onDeleteTopicsResponse(ResponseHeaderData header,
                                                                         DeleteTopicsResponseData response,
                                                                         FilterContext context) {
        return context.forwardResponse(header, mergePartialResponse(header, response));
    }

    @NonNull
    private static IllegalStateException topicIdsNotSupported() {
        return new IllegalStateException("Topic ids not supported yet");
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        return switch (apiKey) {
            case PRODUCE -> onProduceRequest(header, (ProduceRequestData) request, context);
            case FETCH -> onFetchRequest(header, (FetchRequestData) request, context);
            case METADATA -> onMetadataRequest(header, (MetadataRequestData) request, context);
            case CREATE_TOPICS -> onCreateTopicsRequest(header, (CreateTopicsRequestData) request, context);
            case DELETE_TOPICS -> onDeleteTopicsRequest(header, (DeleteTopicsRequestData) request, context);
            case OFFSET_FETCH -> onOffsetFetchRequest(header, (OffsetFetchRequestData) request, context);

            // TODO WRITE: InitProducerId, AddPartitionsToTxn
            // TODO READ: OffsetCommit, TxnOffsetCommit, OffsetDelete
            // TODO ALTER: AlterConfigs, IncrementalAlterConfigs, CreatePartitions
            default -> context.forwardRequest(header, request);
        };
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        return switch (apiKey) {
            case API_VERSIONS -> checkCompat(header, (ApiVersionsResponseData) response, context);
            case PRODUCE -> onProduceResponse(header, (ProduceResponseData) response, context);
            case FETCH -> onFetchResponse(header, (FetchResponseData) response, context);
            case METADATA -> onMetadataResponse(header, (MetadataResponseData) response, context);
            case CREATE_TOPICS -> onCreateTopicsResponse(header, (CreateTopicsResponseData) response, context);
            case DELETE_TOPICS -> onDeleteTopicsResponse(header, (DeleteTopicsResponseData) response, context);

            // TODO DESCRIBE: DescribeTopicPartitions, ListOffset, OffsetFetch, OffsetFetchForLeaderEpoch,
            // TODO DESCRIBE: ConsumerGroupHeartbeat, ConsumerGroupDescribe, ConsumerGroupDescribe
            // TODO DESCRIBE: DescribeProducers,
            // TODO READ: ShareFetch, ShareGroupFetch, ShareAcknowledge, AlterShareGroupOffsets, DeleteShareGroupOffsets,
            // TODO DESCRIBE: ShareGroupHeartbeat, ShareGroupDescribe,
            default -> context.forwardResponse(header, response);
        };
    }

    private CompletionStage<ResponseFilterResult> checkCompat(ResponseHeaderData header, ApiVersionsResponseData response, FilterContext context) {
        ApiVersionsResponseData.ApiVersion apiVersion = response.apiKeys().find(ApiKeys.METADATA.id);
        var minMetadataVersion = apiVersion.minVersion();
        var maxMetadataVersion = apiVersion.maxVersion();
        if (maxMetadataVersion < 4) {
            LOG.error("Filter {} requires the broker to support at least METADATA API version 4. "
                            + "The connected broker supports only {}-{}.",
                    AuthorizationFilter.class.getName(), minMetadataVersion, maxMetadataVersion);
            return context.responseFilterResultBuilder().withCloseConnection().completed();
        }
        this.useMetadataVersion = (short) Math.min(ApiKeys.METADATA.latestVersion(), maxMetadataVersion);
        return context.forwardResponse(header, response);
    }

    private CompletionStage<RequestFilterResult> onOffsetFetchRequest(RequestHeaderData header,
                                                                      OffsetFetchRequestData request,
                                                                      FilterContext context) {
        var isBatched = header.requestApiVersion() >= 8;

        List<Action> actions;
        if (isBatched) {
            actions = TopicResource.DESCRIBE.actionsOf(
                    request.groups().stream()
                            .flatMap(g -> g.topics().stream())
                            .map(OffsetFetchRequestData.OffsetFetchRequestTopics::name));
        }
        else {
            actions = TopicResource.DESCRIBE.actionsOf(
                    request.topics().stream()
                            .map(OffsetFetchRequestData.OffsetFetchRequestTopic::name));
        }
        return authorization(context, actions)
                .thenCompose(authorization -> {
                    if (isBatched) {
                        var t2 = authorization.partition(request.groups().stream()
                                .flatMap(g -> g.topics().stream())
                                .toList(), TopicResource.DESCRIBE, t -> t.name());
                        if (t2.get(Decision.ALLOW).isEmpty()) {
                            return context.requestFilterResultBuilder()
                                    .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                    .completed();
                        }
                        else if (t2.get(Decision.DENY).isEmpty()) {
                            return context.forwardRequest(header, request);
                        }
                        else {
                            List<OffsetFetchResponseData.OffsetFetchResponseGroup> fm = new ArrayList<>();
                            for (var g : request.groups()) {
                                var allowed = g.topics().stream()
                                        .filter(t -> authorization.decision(TopicResource.DESCRIBE, t.name()) == Decision.ALLOW)
                                        .toList();
                                if (allowed.isEmpty()) {
                                    // TODO remove the whole group from the request
                                }
                                List<OffsetFetchResponseData.OffsetFetchResponseTopics> offsetFetchResponseTopics = t2
                                        .get(Decision.DENY).stream().map(x -> new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                                .setName(x.name())
                                                .setPartitions(x.partitionIndexes().stream()
                                                        .map(i -> new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                                                .setPartitionIndex(i)
                                                                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                                                        .toList()))
                                        .toList();
                                fm.add(new OffsetFetchResponseData.OffsetFetchResponseGroup()
                                        .setGroupId(g.groupId())
                                        .setTopics(offsetFetchResponseTopics));
                            }

                            // savePartialResponse(header, (OffsetFetchResponseData response) -> {
                            // // TODO need to write the group merging eugh
                            // for (var g : response.groups()) {
                            // if (g.groupId()
                            // }
                            // response.groups().addAll(fm);
                            // return response;
                            // });
                            // g.setTopics(allowed);
                            return context.forwardRequest(header, request);
                        }
                    }
                    else {
                        var t1 = authorization.partition(request.topics(), TopicResource.DESCRIBE, t -> t.name());
                        if (t1.get(Decision.ALLOW).isEmpty()) {
                            return context.requestFilterResultBuilder()
                                    .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                    .completed();
                        }
                        else if (t1.get(Decision.DENY).isEmpty()) {
                            return context.forwardRequest(header, request);
                        }
                        else {
                            var allowed = request.topics().stream().filter(t -> authorization.decision(TopicResource.DESCRIBE, t.name()) == Decision.ALLOW).toList();
                            var offsetFetchResponseTopics = t1.get(Decision.DENY).stream().map(x -> new OffsetFetchResponseData.OffsetFetchResponseTopic()
                                    .setName(x.name())
                                    .setPartitions(x.partitionIndexes().stream()
                                            .map(i -> new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                                    .setPartitionIndex(i)
                                                    .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                                            .toList()))
                                    .toList();
                            savePartialResponse(header, (OffsetFetchResponseData response) -> {
                                response.topics().addAll(offsetFetchResponseTopics);
                                return response;
                            });
                            request.setTopics(allowed);
                            return context.forwardRequest(header, request);
                        }
                    }
                });
    }

}
