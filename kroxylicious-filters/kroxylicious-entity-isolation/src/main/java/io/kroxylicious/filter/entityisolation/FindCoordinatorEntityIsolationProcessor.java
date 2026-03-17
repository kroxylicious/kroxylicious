/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;
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
        implements EntityIsolationProcessor<FindCoordinatorRequestData, FindCoordinatorResponseData, EntityIsolation.EntityType> {

    private final Predicate<EntityIsolation.EntityType> shouldMap;
    private final EntityNameMapper mapper;

    FindCoordinatorEntityIsolationProcessor(Predicate<EntityType> shouldMap, EntityNameMapper mapper) {
        this.shouldMap = shouldMap;
        this.mapper = mapper;
    }

    @Override
    public short minSupportedVersion() {
        return 0;
    }

    @Override
    public short maxSupportedVersion() {
        return 6;
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
        var resourceType = getResourceType(request.keyType());
        resourceType.filter(shouldMap)
                .ifPresent(rt -> {
                    if ((short) 0 <= header.requestApiVersion() && header.requestApiVersion() <= (short) 3) {
                        request.setKey(mapper.map(mapperContext, rt, request.key()));
                    }
                    else {
                        request.setCoordinatorKeys(request.coordinatorKeys().stream().map(k -> mapper.map(mapperContext, rt, k)).toList());
                    }
                });

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
                                                            @Nullable EntityIsolation.EntityType requestedResourceType,
                                                            FindCoordinatorResponseData response,
                                                            FilterContext filterContext,
                                                            MapperContext mapperContext) {
        Optional.ofNullable(requestedResourceType)
                .filter(shouldMap)
                .ifPresent(resourceType -> response.coordinators()
                        .forEach(coordinator -> coordinator.setKey(mapper.unmap(mapperContext, requestedResourceType, coordinator.key()))));

        return filterContext.forwardResponse(header, response);
    }

    @Nullable
    @Override
    public EntityType createCorrelatedRequestContext(FindCoordinatorRequestData request) {
        return getResourceType(request.keyType()).orElse(null);
    }

    private Optional<EntityType> getResourceType(byte id) {
        var coordinatorType = CoordinatorType.forId(id);
        return convertCoordinatorType(coordinatorType);
    }

    private Optional<EntityType> convertCoordinatorType(CoordinatorType coordinatorType) {
        return switch (coordinatorType) {
            case GROUP, SHARE -> Optional.of(EntityType.GROUP_ID);
            case TRANSACTION -> Optional.of(EntityType.TRANSACTIONAL_ID);
        };
    }

}
