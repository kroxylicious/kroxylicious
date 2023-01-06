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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsResponseFilter;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
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
import io.kroxylicious.proxy.internal.filter.FilterConfig;

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
        OffsetForLeaderEpochRequestFilter, OffsetForLeaderEpochResponseFilter {

    public static class MultiTenantTransformationFilterConfig extends FilterConfig {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantTransformationFilter.class);

    @Override
    public void onCreateTopicsRequest(CreateTopicsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onCreateTopicsResponse(CreateTopicsResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onDeleteTopicsRequest(DeleteTopicsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, topic.topicId() != null));
        context.forwardRequest(request);
    }

    @Override
    public void onDeleteTopicsResponse(DeleteTopicsResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onMetadataRequest(MetadataRequestData request, KrpcFilterContext context) {
        if (request.topics() != null) {
            // n.b. request.topics() == null used to query all the topics.
            request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        }
        context.forwardRequest(request);

    }

    @Override
    public void onMetadataResponse(MetadataResponseData response, KrpcFilterContext context) {
        String tenantPrefix = getTenantPrefix(context);
        response.topics().removeIf(topic -> !topic.name().startsWith(tenantPrefix)); // TODO: allow kafka internal topics to be returned?
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onProduceRequest(ProduceRequestData request, KrpcFilterContext context) {
        request.topicData().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardRequest(request);

    }

    @Override
    public void onProduceResponse(ProduceResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);

    }

    @Override
    public void onListOffsetsRequest(ListOffsetsRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onListOffsetsResponse(ListOffsetsResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetFetchRequest(OffsetFetchRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        request.groups().forEach(requestGroup -> requestGroup.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false)));
        context.forwardRequest(request);
    }

    @Override
    public void onOffsetFetchResponse(OffsetFetchResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        response.groups().forEach(responseGroup -> responseGroup.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false)));
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetForLeaderEpochRequest(OffsetForLeaderEpochRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::topic, topic::setTopic, false));
        context.forwardRequest(request);
    }

    @Override
    public void onOffsetForLeaderEpochResponse(OffsetForLeaderEpochResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::topic, topic::setTopic, false));
        context.forwardResponse(response);
    }

    @Override
    public void onOffsetCommitRequest(OffsetCommitRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardRequest(request);
    }

    @Override
    public void onOffsetCommitResponse(OffsetCommitResponseData response, KrpcFilterContext context) {
        response.topics().forEach(topic -> removeTenantPrefix(context, topic::name, topic::setName, false));
        context.forwardResponse(response);
    }

    @Override
    public void onFetchRequest(FetchRequestData request, KrpcFilterContext context) {
        request.topics().forEach(topic -> applyTenantPrefix(context, topic::topic, topic::setTopic, topic.topicId() != null));
        context.forwardRequest(request);
    }

    @Override
    public void onFetchResponse(FetchResponseData response, KrpcFilterContext context) {
        response.responses().forEach(topic -> removeTenantPrefix(context, topic::topic, topic::setTopic, topic.topicId() != null));
        context.forwardResponse(response);

    }

    private void applyTenantPrefix(KrpcFilterContext context, Supplier<String> getter, Consumer<String> setter, boolean ignoreEmpty) {
        var tenantPrefix = getTenantPrefix(context);
        String clientSideName = getter.get();
        if (ignoreEmpty && (clientSideName == null || clientSideName.isEmpty())) {
            return;
        }
        setter.accept(String.format("%s-%s", tenantPrefix, clientSideName));
    }

    private void removeTenantPrefix(KrpcFilterContext context, Supplier<String> getter, Consumer<String> setter, boolean ignoreEmpty) {
        var tenantPrefix = getTenantPrefix(context);
        var brokerSideName = getter.get();

        if (ignoreEmpty && (brokerSideName == null || brokerSideName.isEmpty())) {
            return;
        }

        setter.accept(brokerSideName.substring(tenantPrefix.length() + 1));
    }

    private static String getTenantPrefix(KrpcFilterContext context) {
        return context.sniHostname().split("\\.")[0];
    }

    public MultiTenantTransformationFilter() {
    }
}
