/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.entityisolation.EntityIsolation.ResourceType;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Entity isolation processor for FIND_COORDINATOR.
 * This implementation is handwritten as at v2, filtering the response requires information
 * from the request.
 */
class FindCoordinatorEntityIsolationProcessor
        implements EntityIsolationProcessor<FindCoordinatorRequestData, FindCoordinatorResponseData, ResourceType> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindCoordinatorEntityIsolationProcessor.class);

    private final Set<ResourceType> resourceTypes;
    private final EntityNameMapper mapper;

    FindCoordinatorEntityIsolationProcessor(Set<ResourceType> resourceTypes, EntityNameMapper mapper) {
        this.resourceTypes = resourceTypes;
        this.mapper = mapper;
    }

    @Override
    public boolean shouldHandleRequest(short apiVersion) {
        return true;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                          short apiVersion,
                                                          FindCoordinatorRequestData request,
                                                          FilterContext filterContext,
                                                          MapperContext mapperContext) {
        log(filterContext, "request", ApiKeys.FIND_COORDINATOR, request);
        var resourceType = getResourceType(request.keyType());
        resourceType.filter(resourceTypes::contains)
                .ifPresent(rt -> {
                    if ((short) 0 <= header.requestApiVersion() && header.requestApiVersion() <= (short) 3) {
                        request.setKey(mapper.map(mapperContext, rt, request.key()));
                    }
                    else {
                        request.setCoordinatorKeys(request.coordinatorKeys().stream().map(k -> mapper.map(mapperContext, rt, k)).toList());
                    }
                });

        log(filterContext, "request result", ApiKeys.FIND_COORDINATOR, request);
        return filterContext.forwardRequest(header, request);
    }

    @Override
    public boolean shouldHandleResponse(short apiVersion) {
        return true;
    }

    @Override
    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                            short apiVersion,
                                                            @Nullable ResourceType requestedResourceType,
                                                            FindCoordinatorResponseData response,
                                                            FilterContext filterContext,
                                                            MapperContext mapperContext) {
        log(filterContext, "response", ApiKeys.FIND_COORDINATOR, response);
        Optional.ofNullable(requestedResourceType)
                .filter(resourceTypes::contains)
                .ifPresent(resourceType -> response.coordinators()
                        .forEach(coordinator -> coordinator.setKey(mapper.unmap(mapperContext, requestedResourceType, coordinator.key()))));

        log(filterContext, "response result", ApiKeys.FIND_COORDINATOR, response);

        return filterContext.forwardResponse(header, response);
    }

    private static void log(FilterContext context, String description, ApiKeys key, ApiMessage message) {
        LOGGER.atDebug()
                .addArgument(() -> context.sessionId())
                .addArgument(() -> context.authenticatedSubject())
                .addArgument(description)
                .addArgument(key)
                .addArgument(message)
                .log("{} for {}: {} {}: {}");
    }

    @Nullable
    @Override
    public ResourceType createCorrelatedRequestContext(FindCoordinatorRequestData request) {
        return getResourceType(request.keyType()).orElse(null);
    }

    private Optional<ResourceType> getResourceType(byte id) {
        var coordinatorType = CoordinatorType.forId(id);
        return convertCoordinatorType(coordinatorType);
    }

    private Optional<ResourceType> convertCoordinatorType(CoordinatorType coordinatorType) {
        return switch (coordinatorType) {
            case GROUP, SHARE -> Optional.of(ResourceType.GROUP_ID);
            case TRANSACTION -> Optional.of(ResourceType.TRANSACTIONAL_ID);
        };
    }

}
