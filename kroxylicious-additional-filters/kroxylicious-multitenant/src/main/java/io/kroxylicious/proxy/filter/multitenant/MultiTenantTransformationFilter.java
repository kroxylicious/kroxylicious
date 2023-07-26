/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.multitenant;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.AddOffsetsToTxnRequestFilter;
import io.kroxylicious.proxy.filter.AddPartitionsToTxnRequestFilter;
import io.kroxylicious.proxy.filter.AddPartitionsToTxnResponseFilter;
import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeGroupsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeGroupsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeTransactionsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeTransactionsResponseFilter;
import io.kroxylicious.proxy.filter.EndTxnRequestFilter;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterResult;
import io.kroxylicious.proxy.filter.FindCoordinatorRequestFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorResponseFilter;
import io.kroxylicious.proxy.filter.HeartbeatRequestFilter;
import io.kroxylicious.proxy.filter.InitProducerIdRequestFilter;
import io.kroxylicious.proxy.filter.JoinGroupRequestFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.LeaveGroupRequestFilter;
import io.kroxylicious.proxy.filter.ListGroupsResponseFilter;
import io.kroxylicious.proxy.filter.ListOffsetsRequestFilter;
import io.kroxylicious.proxy.filter.ListOffsetsResponseFilter;
import io.kroxylicious.proxy.filter.ListTransactionsResponseFilter;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.OffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.OffsetCommitResponseFilter;
import io.kroxylicious.proxy.filter.OffsetDeleteRequestFilter;
import io.kroxylicious.proxy.filter.OffsetDeleteResponseFilter;
import io.kroxylicious.proxy.filter.OffsetFetchRequestFilter;
import io.kroxylicious.proxy.filter.OffsetFetchResponseFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochRequestFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochResponseFilter;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.SyncGroupRequestFilter;
import io.kroxylicious.proxy.filter.TxnOffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.TxnOffsetCommitResponseFilter;

/**
 * Simple multi-tenant filter.
 *
 * Uses the first component of a fully-qualified host name as a tenant prefix.
 * This tenant prefix is prepended to the kafka resources name in order to present an isolated
 * environment for each tenant.
 *
 * TODO prefix other resources e.g. group names, transaction ids
 * TODO disallow the use of topic uids belonging to one tenant by another.
 */
public class MultiTenantTransformationFilter
        implements CreateTopicsRequestFilter, CreateTopicsResponseFilter,
        DeleteTopicsRequestFilter, DeleteTopicsResponseFilter,
        MetadataRequestFilter, MetadataResponseFilter,
        ProduceRequestFilter, ProduceResponseFilter,
        ListOffsetsRequestFilter, ListOffsetsResponseFilter,
        FetchRequestFilter, FetchResponseFilter,
        OffsetFetchRequestFilter, OffsetFetchResponseFilter,
        OffsetCommitRequestFilter, OffsetCommitResponseFilter,
        OffsetDeleteRequestFilter, OffsetDeleteResponseFilter,
        OffsetForLeaderEpochRequestFilter, OffsetForLeaderEpochResponseFilter,
        FindCoordinatorRequestFilter, FindCoordinatorResponseFilter,
        ListGroupsResponseFilter,
        JoinGroupRequestFilter,
        SyncGroupRequestFilter,
        LeaveGroupRequestFilter,
        HeartbeatRequestFilter,
        DescribeGroupsRequestFilter, DescribeGroupsResponseFilter,
        InitProducerIdRequestFilter,
        ListTransactionsResponseFilter,
        DescribeTransactionsRequestFilter, DescribeTransactionsResponseFilter,
        EndTxnRequestFilter,
        AddPartitionsToTxnRequestFilter, AddPartitionsToTxnResponseFilter,
        AddOffsetsToTxnRequestFilter,
        TxnOffsetCommitRequestFilter, TxnOffsetCommitResponseFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantTransformationFilter.class);

    @Override
    public CompletionStage<? extends FilterResult> onCreateTopicsRequest(short apiVersion, RequestHeaderData header, CreateTopicsRequestData request,
                                                                         KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onCreateTopicsResponse(short apiVersion, ResponseHeaderData header, CreateTopicsResponseData response,
                                                                        KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onDeleteTopicsRequest(short apiVersion, RequestHeaderData header, DeleteTopicsRequestData request,
                                                                         KrpcFilterContext context) {
        // the topicName field was present up to and including version 5
        request.setTopicNames(request.topicNames().stream().map(topic -> applyTenantPrefix(context, topic)).toList());
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, topic.topicId() != null));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onDeleteTopicsResponse(short apiVersion, ResponseHeaderData header, DeleteTopicsResponseData response,
                                                                        KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onMetadataRequest(short apiVersion, RequestHeaderData header, MetadataRequestData request, KrpcFilterContext context) {
        if (request.topics() != null) {
            // n.b. request.topics() == null used to query all the topics.
            request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        }
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response,
                                                                    KrpcFilterContext context) {
        String tenantPrefix = getTenantPrefix(context);
        response.topics().removeIf(topic -> !topic.name().startsWith(tenantPrefix)); // TODO: allow kafka internal topics to be returned?
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
        applyTenantPrefix(context, request::transactionalId, request::setTransactionalId, true);
        request.topicData().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onProduceResponse(short apiVersion, ResponseHeaderData header, ProduceResponseData response,
                                                                   KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onListOffsetsRequest(short apiVersion, RequestHeaderData header, ListOffsetsRequestData request,
                                                                        KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onListOffsetsResponse(short apiVersion, ResponseHeaderData header, ListOffsetsResponseData response,
                                                                       KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onOffsetFetchRequest(short apiVersion, RequestHeaderData header, OffsetFetchRequestData request,
                                                                        KrpcFilterContext context) {
        // the groupId and top-level topic fields were present up to and including version 7
        Optional.ofNullable(request.groupId()).ifPresent(groupId -> applyTenantPrefix(context, request::groupId, request::setGroupId, true));
        Optional.ofNullable(request.topics()).ifPresent(topics -> topics.forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false)));
        request.groups().forEach(requestGroup -> {
            applyTenantPrefix(context, requestGroup::groupId, requestGroup::setGroupId, false);
            Optional.ofNullable(requestGroup.topics())
                    .ifPresent(topics -> topics.forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false)));
        });

        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetFetchResponse(short apiVersion, ResponseHeaderData header, OffsetFetchResponseData response,
                                                                       KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        response.groups().forEach(responseGroup -> {
            removeTenantPrefix(context, responseGroup::groupId, responseGroup::setGroupId, false);
            responseGroup.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        });
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onOffsetForLeaderEpochRequest(short apiVersion, RequestHeaderData header, OffsetForLeaderEpochRequestData request,
                                                                                 KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::topic, topic::setTopic, false));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetForLeaderEpochResponse(short apiVersion, ResponseHeaderData header, OffsetForLeaderEpochResponseData response,
                                                                                KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::topic, topic::setTopic, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onOffsetCommitRequest(short apiVersion, RequestHeaderData header, OffsetCommitRequestData request,
                                                                         KrpcFilterContext context) {
        applyTenantPrefix(context, request::groupId, request::setGroupId, false);
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetCommitResponse(short apiVersion, ResponseHeaderData header, OffsetCommitResponseData response,
                                                                        KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onOffsetDeleteRequest(short apiVersion, RequestHeaderData header, OffsetDeleteRequestData request,
                                                                         KrpcFilterContext context) {
        applyTenantPrefix(context, request::groupId, request::setGroupId, false);
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onOffsetDeleteResponse(short apiVersion, ResponseHeaderData header, OffsetDeleteResponseData response,
                                                                        KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onFetchRequest(short apiVersion, RequestHeaderData header, FetchRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::topic, topic::setTopic, topic.topicId() != null));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::topic, topic::setTopic, topic.topicId() != null));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onFindCoordinatorRequest(short apiVersion, RequestHeaderData header, FindCoordinatorRequestData request,
                                                                            KrpcFilterContext context) {
        // the key fields was present up to and including version 4
        Optional.ofNullable(request.key()).ifPresent(unused -> applyTenantPrefix(context, request::key, request::setKey, true));
        request.setCoordinatorKeys(request.coordinatorKeys().stream().map(key -> applyTenantPrefix(context, key)).toList());
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFindCoordinatorResponse(short apiVersion, ResponseHeaderData header, FindCoordinatorResponseData response,
                                                                           KrpcFilterContext context) {
        response.coordinators().forEach(coordinator -> removeTenantPrefix(context, coordinator::key, coordinator::setKey, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onListGroupsResponse(short apiVersion, ResponseHeaderData header, ListGroupsResponseData response,
                                                                      KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        var filteredGroups = response.groups().stream().filter(listedGroup -> listedGroup.groupId().startsWith(tenantPrefix)).toList();
        filteredGroups.forEach(listedGroup -> removeTenantPrefix(context, listedGroup::groupId, listedGroup::setGroupId, false));
        response.setGroups(filteredGroups);
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onJoinGroupRequest(short apiVersion, RequestHeaderData header, JoinGroupRequestData request,
                                                                      KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        request.setGroupId(tenantPrefix + request.groupId());
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<? extends FilterResult> onSyncGroupRequest(short apiVersion, RequestHeaderData header, SyncGroupRequestData request,
                                                                      KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        request.setGroupId(tenantPrefix + request.groupId());
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<? extends FilterResult> onLeaveGroupRequest(short apiVersion, RequestHeaderData header, LeaveGroupRequestData request,
                                                                       KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        request.setGroupId(tenantPrefix + request.groupId());
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<? extends FilterResult> onHeartbeatRequest(short apiVersion, RequestHeaderData header, HeartbeatRequestData request,
                                                                      KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        request.setGroupId(tenantPrefix + request.groupId());
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<? extends FilterResult> onDescribeGroupsRequest(short apiVersion, RequestHeaderData header, DescribeGroupsRequestData request,
                                                                           KrpcFilterContext context) {
        request.setGroups(request.groups().stream().map(group -> applyTenantPrefix(context, group)).toList());
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onDescribeGroupsResponse(short apiVersion, ResponseHeaderData header, DescribeGroupsResponseData response,
                                                                          KrpcFilterContext context) {
        response.groups().forEach(group -> removeTenantPrefix(context, group::groupId, group::setGroupId, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onInitProducerIdRequest(short apiVersion, RequestHeaderData header, InitProducerIdRequestData request,
                                                                           KrpcFilterContext context) {
        applyTenantPrefix(context, request::transactionalId, request::setTransactionalId, true);
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<? extends FilterResult> onAddPartitionsToTxnRequest(short apiVersion, RequestHeaderData header, AddPartitionsToTxnRequestData request,
                                                                               KrpcFilterContext context) {
        request.v3AndBelowTopics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        applyTenantPrefix(context, request::v3AndBelowTransactionalId, request::setV3AndBelowTransactionalId, true);

        request.transactions().forEach(addPartitionsToTxnTransaction -> {
            applyTenantPrefix(context, addPartitionsToTxnTransaction::transactionalId, addPartitionsToTxnTransaction::setTransactionalId, true);
            addPartitionsToTxnTransaction.topics().forEach(addPartitionsToTxnTopic -> {
                applyTenantPrefix(context, addPartitionsToTxnTopic::name, addPartitionsToTxnTopic::setName, true);
            });
        });
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onAddPartitionsToTxnResponse(short apiVersion, ResponseHeaderData header, AddPartitionsToTxnResponseData response,
                                                                              KrpcFilterContext context) {
        response.resultsByTopicV3AndBelow().forEach(results -> removeTenantPrefix(context, results::name, results::setName, false));

        response.resultsByTransaction().forEach(addPartitionsToTxnResult -> {
            removeTenantPrefix(context, addPartitionsToTxnResult::transactionalId, addPartitionsToTxnResult::setTransactionalId, false);
            for (AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult topicResult : addPartitionsToTxnResult.topicResults()) {
                removeTenantPrefix(context, topicResult::name, topicResult::setName, true);
            }
        });
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onAddOffsetsToTxnRequest(short apiVersion, RequestHeaderData header, AddOffsetsToTxnRequestData request,
                                                                            KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        request.setTransactionalId(tenantPrefix + request.transactionalId());
        request.setGroupId(tenantPrefix + request.groupId());
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<? extends FilterResult> onTxnOffsetCommitRequest(short apiVersion, RequestHeaderData header, TxnOffsetCommitRequestData request,
                                                                            KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        request.setTransactionalId(tenantPrefix + request.transactionalId());
        request.setGroupId(tenantPrefix + request.groupId());
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onTxnOffsetCommitResponse(short apiVersion, ResponseHeaderData header, TxnOffsetCommitResponseData response,
                                                                           KrpcFilterContext context) {
        response.topics().forEach(results -> removeTenantPrefix(context, results::name, results::setName, false));
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onListTransactionsResponse(short apiVersion, ResponseHeaderData header, ListTransactionsResponseData response,
                                                                            KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        var filteredTransactions = response.transactionStates().stream().filter(listedTxn -> listedTxn.transactionalId().startsWith(tenantPrefix)).toList();
        filteredTransactions.forEach(listedTxn -> removeTenantPrefix(context, listedTxn::transactionalId, listedTxn::setTransactionalId, false));
        response.setTransactionStates(filteredTransactions);
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onDescribeTransactionsRequest(short apiVersion, RequestHeaderData header, DescribeTransactionsRequestData request,
                                                                                 KrpcFilterContext context) {
        request.setTransactionalIds(request.transactionalIds().stream().map(group -> applyTenantPrefix(context, group)).toList());
        return context.completedForwardRequest(request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onDescribeTransactionsResponse(short apiVersion, ResponseHeaderData header, DescribeTransactionsResponseData response,
                                                                                KrpcFilterContext context) {
        response.transactionStates().forEach(ts -> {
            removeTenantPrefix(context, ts::transactionalId, ts::setTransactionalId, false);
            ts.topics().forEach(t -> removeTenantPrefix(context, t::topic, t::setTopic, false));
        });
        return context.completedForwardResponse(response);
    }

    @Override
    public CompletionStage<? extends FilterResult> onEndTxnRequest(short apiVersion, RequestHeaderData header, EndTxnRequestData request, KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        request.setTransactionalId(tenantPrefix + request.transactionalId());
        return context.completedForwardRequest(request);
    }

    private void applyTenantPrefix(KrpcFilterContext context, Supplier<String> getter, Consumer<String> setter, boolean ignoreEmpty) {
        String clientSideName = getter.get();
        if (ignoreEmpty && (clientSideName == null || clientSideName.isEmpty())) {
            return;
        }
        setter.accept(applyTenantPrefix(context, clientSideName));
    }

    private String applyTenantPrefix(KrpcFilterContext context, String clientSideName) {
        var tenantPrefix = getTenantPrefix(context);
        return tenantPrefix + clientSideName;
    }

    private void removeTenantPrefix(KrpcFilterContext context, Supplier<String> getter, Consumer<String> setter, boolean ignoreEmpty) {
        var brokerSideName = getter.get();
        if (ignoreEmpty && (brokerSideName == null || brokerSideName.isEmpty())) {
            return;
        }

        setter.accept(removeTenantPrefix(context, brokerSideName));
    }

    private String removeTenantPrefix(KrpcFilterContext context, String brokerSideName) {
        var tenantPrefix = getTenantPrefix(context);
        return brokerSideName.substring(tenantPrefix.length());
    }

    private static String getTenantPrefix(KrpcFilterContext context) {
        // TODO naive - POC implementation uses the first component of a FQDN as the multi-tenant prefix.
        var sniHostname = context.sniHostname();
        if (sniHostname == null) {
            throw new IllegalStateException("This filter requires that the client provides a TLS SNI hostname.");
        }
        int dot = sniHostname.indexOf(".");
        if (dot < 1) {
            throw new IllegalStateException("Unexpected SNI hostname formation. SNI hostname : " + sniHostname);
        }
        return sniHostname.substring(0, dot) + "-";
    }

    public MultiTenantTransformationFilter() {
    }
}
