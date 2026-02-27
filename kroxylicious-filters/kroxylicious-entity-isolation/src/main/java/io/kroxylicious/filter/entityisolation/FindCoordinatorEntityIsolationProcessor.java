/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class FindCoordinatorEntityIsolationProcessor implements EntityIsolationProcessor<FindCoordinatorRequestData, FindCoordinatorResponseData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindCoordinatorEntityIsolationProcessor.class);

    private final Set<EntityIsolation.ResourceType> resourceTypes;
    private final EntityNameMapper mapper;

    public FindCoordinatorEntityIsolationProcessor(Set<EntityIsolation.ResourceType> resourceTypes, EntityNameMapper mapper) {
        this.resourceTypes = resourceTypes;
        this.mapper = mapper;
    }

    @Override
    public boolean shouldHandleRequest(short apiVersion) {
        return true;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, short apiVersion, FindCoordinatorRequestData request, FilterContext filterContext,
                                                          MapperContext mapperContext) {
        log(filterContext, "request", ApiKeys.FIND_COORDINATOR, request);
        // TODO handle FindCoordinatorRequest.CoordinatorType.TRANSACTION
        // TODO handle FindCoordinatorRequest.CoordinatorType.SHARE which uses a key in the format "groupId:topicId:partition"
        // TODO the response does not include the coordinator type, so we'll need to cache the request like the authorization filter does,\
        if (resourceTypes.contains(EntityIsolation.ResourceType.GROUP_ID) && request.keyType() == FindCoordinatorRequest.CoordinatorType.GROUP.id()) {

            if ((short) 0 <= header.requestApiVersion() && header.requestApiVersion() <= (short) 3) {
                request.setKey(map(mapperContext, EntityIsolation.ResourceType.GROUP_ID, request.key()));
            }
            else {
                request.setCoordinatorKeys(request.coordinatorKeys().stream().map(k -> map(mapperContext, EntityIsolation.ResourceType.GROUP_ID, k)).toList());
            }
        }

        log(filterContext, "request result", ApiKeys.FIND_COORDINATOR, request);
        return filterContext.forwardRequest(header, request);
    }

    @Override
    public boolean shouldHandleResponse(short apiVersion) {
        return true;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header, short apiVersion, FindCoordinatorResponseData response,
                                                            FilterContext filterContext,
                                                            MapperContext mapperContext) {
        log(filterContext, "response", ApiKeys.FIND_COORDINATOR, response);
        response.coordinators().forEach(
                coordinator -> coordinator.setKey(unmap(mapperContext, EntityIsolation.ResourceType.GROUP_ID, coordinator.key())));
        log(filterContext, "response result", ApiKeys.FIND_COORDINATOR, response);

        return filterContext.forwardResponse(header, response);
    }

    private String map(MapperContext context, EntityIsolation.ResourceType resourceType, String originalName) {
        if (originalName == null || originalName.isEmpty()) {
            return originalName;
        }
        return mapper.map(context, resourceType, originalName);
    }

    private String unmap(MapperContext context, EntityIsolation.ResourceType resourceType, String mappedName) {
        if (mappedName.isEmpty()) {
            return mappedName;
        }
        return mapper.unmap(context, resourceType, mappedName);
    }

    private boolean inNamespace(MapperContext context, EntityIsolation.ResourceType resourceType, String mappedName) {
        return mapper.isInNamespace(context, resourceType, mappedName);
    }

    private static MapperContext buildMapperContext(FilterContext context) {
        return new MapperContext(context.authenticatedSubject(),
                context.clientTlsContext().orElse(null),
                context.clientSaslContext().orElse(null));
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

}
