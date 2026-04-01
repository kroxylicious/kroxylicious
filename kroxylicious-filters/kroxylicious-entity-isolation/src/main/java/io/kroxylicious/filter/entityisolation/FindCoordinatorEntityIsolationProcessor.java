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

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Entity isolation processor for FIND_COORDINATOR.
 * This implementation is handwritten as at v2, filtering the response requires information
 * from the request.
 */
class FindCoordinatorEntityIsolationProcessor
        implements EntityIsolationProcessor<FindCoordinatorRequestData, FindCoordinatorResponseData, CoordinatorType> {

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
        var coordinatorType = CoordinatorType.forId(request.keyType());
        var entityType = convertCoordinatorType(coordinatorType);
        entityType.filter(shouldMap)
                .ifPresent(et -> {
                    if ((short) 0 <= header.requestApiVersion() && header.requestApiVersion() <= (short) 3) {
                        request.setKey(doMap(mapperContext, coordinatorType, et, request.key()));
                    }
                    else {
                        request.setCoordinatorKeys(request.coordinatorKeys().stream().map(k -> doMap(mapperContext, coordinatorType, et, k)).toList());
                    }
                });

        return filterContext.forwardRequest(header, request);
    }

    private String doMap(MapperContext mapperContext,
                         CoordinatorType coordinatorType,
                         EntityType entityType,
                         String key) {
        if (coordinatorType == CoordinatorType.SHARE) {
            var split = splitShareGroupKey(key);
            split[0] = mapper.map(mapperContext, entityType, split[0]);
            return String.join(":", split);
        }
        else {
            return mapper.map(mapperContext, entityType, key);
        }
    }

    @Override
    public boolean shouldHandleResponse(short apiVersion) {
        return true;
    }

    @Override
    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                            short apiVersion,
                                                            @NonNull CoordinatorType requestedCoordinatorType,
                                                            FindCoordinatorResponseData response,
                                                            FilterContext filterContext,
                                                            MapperContext mapperContext) {
        var requestedEntityType = convertCoordinatorType(requestedCoordinatorType);
        requestedEntityType
                .filter(shouldMap)
                .ifPresent(et -> response.coordinators()
                        .forEach(coordinator -> coordinator.setKey(doUnmap(et, requestedCoordinatorType, mapperContext, coordinator.key()))));

        return filterContext.forwardResponse(header, response);
    }

    private String doUnmap(EntityType requestedEntityType,
                           CoordinatorType coordinatorType,
                           MapperContext mapperContext,
                           String key) {
        if (coordinatorType == CoordinatorType.SHARE) {
            // From FindCoordinatorRequest.json: For key type SHARE (2), the coordinator key format is "groupId:topicId:partition".
            var split = splitShareGroupKey(key);
            split[0] = mapper.unmap(mapperContext, requestedEntityType, split[0]);
            return String.join(":", split);
        }
        else {
            return mapper.unmap(mapperContext, requestedEntityType, key);
        }
    }

    @Override
    public CoordinatorType createCorrelatedRequestContext(FindCoordinatorRequestData request) {
        return CoordinatorType.forId(request.keyType());
    }

    private Optional<EntityType> convertCoordinatorType(CoordinatorType coordinatorType) {
        return switch (coordinatorType) {
            case GROUP, SHARE -> Optional.of(EntityType.GROUP_ID);
            case TRANSACTION -> Optional.of(EntityType.TRANSACTIONAL_ID);
        };
    }

    private static String[] splitShareGroupKey(String key) {
        // From FindCoordinatorRequest.json: For key type SHARE (2), the coordinator key format is "groupId:topicId:partition".
        var split = key.split(":", 3);
        if (split.length != 3) {
            throw new IllegalStateException(String.format("Invalid formation of share group key '%s'", key));
        }
        return split;
    }
}
