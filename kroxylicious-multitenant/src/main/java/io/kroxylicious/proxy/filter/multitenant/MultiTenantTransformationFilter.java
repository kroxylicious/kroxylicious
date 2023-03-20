/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.multitenant;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsResponseFilter;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorRequestFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ListGroupsRequestFilter;
import io.kroxylicious.proxy.filter.ListGroupsResponseFilter;
import io.kroxylicious.proxy.filter.ListOffsetsRequestFilter;
import io.kroxylicious.proxy.filter.ListOffsetsResponseFilter;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.OffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.OffsetCommitResponseFilter;
import io.kroxylicious.proxy.filter.OffsetFetchRequestFilter;
import io.kroxylicious.proxy.filter.OffsetFetchResponseFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochRequestFilter;
import io.kroxylicious.proxy.filter.OffsetForLeaderEpochResponseFilter;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;

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
        OffsetForLeaderEpochRequestFilter, OffsetForLeaderEpochResponseFilter,
        FindCoordinatorRequestFilter, FindCoordinatorResponseFilter,
        ListGroupsRequestFilter, ListGroupsResponseFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantTransformationFilter.class);

    @Override
    public void onCreateTopicsRequest(RequestHeaderData data, CreateTopicsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onCreateTopicsResponse(ResponseHeaderData data, CreateTopicsResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onDeleteTopicsRequest(RequestHeaderData data, DeleteTopicsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, topic.topicId() != null));
        context.forwardRequest(request);
    }

    @Override
    public void onDeleteTopicsResponse(ResponseHeaderData data, DeleteTopicsResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onMetadataRequest(RequestHeaderData data, MetadataRequestData request, KrpcFilterContext context) {
        if (request.topics() != null) {
            // n.b. request.topics() == null used to query all the topics.
            request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        }
        context.forwardRequest(request);

    }

    @Override
    public void onMetadataResponse(ResponseHeaderData data, MetadataResponseData response, KrpcFilterContext context) {
        String tenantPrefix = getTenantPrefix(context);
        response.topics().removeIf(topic -> !topic.name().startsWith(tenantPrefix)); // TODO: allow kafka internal topics to be returned?
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onProduceRequest(RequestHeaderData data, ProduceRequestData request, KrpcFilterContext context) {
        request.topicData().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardRequest(request);

    }

    @Override
    public void onProduceResponse(ResponseHeaderData data, ProduceResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);

    }

    @Override
    public void onListOffsetsRequest(RequestHeaderData data, ListOffsetsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onListOffsetsResponse(ResponseHeaderData data, ListOffsetsResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetFetchRequest(RequestHeaderData data, OffsetFetchRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        request.groups().forEach(requestGroup -> {
            applyTenantPrefix(context, requestGroup::groupId, requestGroup::setGroupId, false);
            requestGroup.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        });

        context.forwardRequest(request);
    }

    @Override
    public void onOffsetFetchResponse(ResponseHeaderData data, OffsetFetchResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        response.groups().forEach(responseGroup -> {
            removeTenantPrefix(context, responseGroup::groupId, responseGroup::setGroupId, false);
            responseGroup.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        });
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetForLeaderEpochRequest(RequestHeaderData data, OffsetForLeaderEpochRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::topic, topic::setTopic, false));
        context.forwardRequest(request);
    }

    @Override
    public void onOffsetForLeaderEpochResponse(ResponseHeaderData data, OffsetForLeaderEpochResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::topic, topic::setTopic, false));
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetCommitRequest(RequestHeaderData data, OffsetCommitRequestData request, KrpcFilterContext context) {
        applyTenantPrefix(context, request::groupId, request::setGroupId, false);
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onOffsetCommitResponse(ResponseHeaderData data, OffsetCommitResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onFetchRequest(RequestHeaderData data, FetchRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::topic, topic::setTopic, topic.topicId() != null));
        context.forwardRequest(request);
    }

    @Override
    public void onFetchResponse(ResponseHeaderData data, FetchResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::topic, topic::setTopic, topic.topicId() != null));
        context.forwardResponse(response);
    }

    @Override
    public void onFindCoordinatorRequest(RequestHeaderData header, FindCoordinatorRequestData request, KrpcFilterContext context) {
        request.setCoordinatorKeys(request.coordinatorKeys().stream().map(key -> applyTenantPrefix(context, key)).toList());
        context.forwardRequest(request);
    }

    @Override
    public void onFindCoordinatorResponse(ResponseHeaderData header, FindCoordinatorResponseData response, KrpcFilterContext context) {
        response.coordinators().forEach(coordinator -> removeTenantPrefix(context, coordinator::key, coordinator::setKey, false));
        context.forwardResponse(response);
    }

    @Override
    public void onListGroupsRequest(RequestHeaderData header, ListGroupsRequestData request, KrpcFilterContext context) {
        context.forwardRequest(request);
    }

    @Override
    public void onListGroupsResponse(ResponseHeaderData header, ListGroupsResponseData response, KrpcFilterContext context) {
        var tenantPrefix = getTenantPrefix(context);
        var filteredGroups = response.groups().stream().filter(listedGroup -> listedGroup.groupId().startsWith(tenantPrefix)).toList();
        filteredGroups.forEach(listedGroup -> removeTenantPrefix(context, listedGroup::groupId, listedGroup::setGroupId, false));
        response.setGroups(filteredGroups);
        context.forwardResponse(response);
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
        return tenantPrefix + "-" + clientSideName;
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
        return brokerSideName.substring(tenantPrefix.length() + 1);
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
        return sniHostname.substring(0, dot);
    }

    public MultiTenantTransformationFilter() {
    }

}
