/*
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
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

    private final Authorizer authorizer;
    private final Map<Integer, UnaryOperator<?>> bufferedPartialResponses;
    private final Map<String, Object> topicCache;

    public AuthorizationFilter(Authorizer authorizer,
                               Map<String, Object> topicCache) {
        this.authorizer = authorizer;
        this.bufferedPartialResponses = new HashMap<>(10);
        this.topicCache = topicCache;
    }

    CompletionStage<Subject> subject(FilterContext context) {
        return context.authenticatedSubject();
    }

    private boolean isAllTopics(RequestHeaderData header, MetadataRequestData metadataRequest) {
        return (metadataRequest.topics() == null) ||
                (metadataRequest.topics().isEmpty() && header.requestApiVersion() == 0);
    }

    public CompletionStage<RequestFilterResult> onMetadataRequest(RequestHeaderData header,
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
        // creation, filter the response with our authorizer
        // and then forward it again without the _disallowed_ topic creations
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
                    Authorization createAuthorization = subject(context)
                            .thenCompose(subject -> authorizer.authorize(subject, createActions)).toCompletableFuture().join();
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
                        // TODO write the other half of this!
                        savePartialResponse(header, (MetadataResponseData response) -> {
                            response.topics().addAll(forBuffering);
                            return response;
                        });

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

    public CompletionStage<ResponseFilterResult> onMetadataResponse(ResponseHeaderData header,
                                                                    MetadataResponseData response,
                                                                    FilterContext context) {
        List<Action> actionStream = response.topics().stream()
                .map(responseTopic -> new Action(TopicResource.DESCRIBE, responseTopic.name()))
                .toList();
        Authorization authorize = subject(context)
                .thenCompose(subject -> authorizer.authorize(subject, actionStream)).toCompletableFuture().join();
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

    public CompletionStage<RequestFilterResult> onProduceRequest(RequestHeaderData header,
                                                                 ProduceRequestData request,
                                                                 FilterContext context) {

        var topicWriteActions = request.topicData().stream()
                .map(t -> new Action(TopicResource.WRITE, t.name()))
                .toList();

        Authorization authorize = subject(context)
                .thenCompose(subject -> authorizer.authorize(subject, topicWriteActions)).toCompletableFuture().join();

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

        var topicProduceResponses = deniedTopicWrites.stream().map(topicProduceData -> {
            return new ProduceResponseData.TopicProduceResponse().setName(topicProduceData.name())
                    .setPartitionResponses(topicProduceData.partitionData().stream().map(partitionProduceData ->
                                    new ProduceResponseData.PartitionProduceResponse()
                                            .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message())
                                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                            .toList());
        }).toList();

        savePartialResponse(header, (ProduceResponseData response) -> {
            response.responses().addAll(topicProduceResponses);
            return response;
        });
        return context.forwardRequest(header, filteredRequest);
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
        return subject(context)
                .thenCompose(subject -> authorizer.authorize(subject, topicReadActions))
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
                                        .setPartitions(t.partitions().stream().map(p ->
                                            new FetchResponseData.PartitionData()
                                                    .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                                        ).toList())
                                )
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
        return subject(context)
                .thenCompose(subject -> authorizer.authorize(subject, topicReadActions))
                .thenCompose(authorization -> {
                    var topicReadDecisions = authorization.partition(request.topics(),
                            TopicResource.CREATE,
                            CreateTopicsRequestData.CreatableTopic::name);
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
                        request.setTopics(new CreateTopicsRequestData.CreatableTopicCollection(topicReadDecisions.get(Decision.ALLOW).iterator()));
                        var creatableTopicResults = topicReadDecisions.get(Decision.DENY)
                                .stream().map(t -> new CreateTopicsResponseData.CreatableTopicResult()
                                        .setName(t.name())
                                        .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                                        .setErrorMessage(header.requestApiVersion() >= 1 ? Errors.TOPIC_AUTHORIZATION_FAILED.message() : null)
                                        ).toList();
                        savePartialResponse(header, (CreateTopicsResponseData response) -> {
                            response.topics().addAll(creatableTopicResults);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    /**
     * Filter out any topic configs if the subject lacks DESCRIBE_CONFIGS
     * Append responses buffered when checking the request
     */
    private CompletionStage<ResponseFilterResult> onCreateTopicsResponse(ResponseHeaderData header,
                                                                         CreateTopicsResponseData response,
                                                                         FilterContext context) {

        List<Action> actions = TopicResource.DESCRIBE_CONFIGS.actionsOf(response.topics().stream().map(t -> t.name()));
        return subject(context)
                .thenCompose(subject -> authorizer.authorize(subject, actions))
                .thenCompose(authorization -> {
            for (var creatableTopicResult : response.topics()) {
                if (authorization.decision(TopicResource.DESCRIBE_CONFIGS, creatableTopicResult.name()) == Decision.DENY) {
                    creatableTopicResult.setConfigs(null);
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
        return subject(context)
                .thenCompose(subject -> authorizer.authorize(subject, actions))
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
                                            .setErrorMessage(apiVersion >= 5 ? Errors.TOPIC_AUTHORIZATION_FAILED.message() : null)
                                    ).toList();
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
                                            .setErrorMessage(apiVersion >= 5 ? Errors.TOPIC_AUTHORIZATION_FAILED.message() : null)
                                    ).toList();
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
            // TODO READ: OffsetCommit, TxnOffsetCommit, OffsetDelete
            // TODO ALTER: AlterConfigs, IncrementalAlterConfigs, CreatePartitions
            // TODO WRITE: InitProducerId, AddPartitionsToTxn
            default -> context.forwardRequest(header, request);
        };
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        return switch (apiKey) {
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
        return subject(context)
                .thenCompose(subject -> authorizer.authorize(subject, actions))
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
                        List<OffsetFetchResponseData.OffsetFetchResponseTopics> offsetFetchResponseTopics = t2.get(Decision.DENY).stream().map(x -> new OffsetFetchResponseData.OffsetFetchResponseTopics()
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

//                    savePartialResponse(header, (OffsetFetchResponseData response) -> {
//                        // TODO need to write the group merging eugh
//                        for (var g : response.groups()) {
//                            if (g.groupId()
//                        }
//                        response.groups().addAll(fm);
//                        return response;
//                    });
//                    g.setTopics(allowed);
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
