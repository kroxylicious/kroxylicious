/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * Entity isolation processor for DELETE_ACLS.
 * This implementation is handwritten as the fields of the RPC doesn't follow a common naming convention.
 */
class DeleteAclsEntityIsolationProcessor implements EntityIsolationProcessor<DeleteAclsRequestData, DeleteAclsResponseData, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteAclsEntityIsolationProcessor.class);

    private final Set<EntityIsolation.ResourceType> resourceTypes;
    private final EntityNameMapper mapper;

    DeleteAclsEntityIsolationProcessor(Set<EntityIsolation.ResourceType> resourceTypes, EntityNameMapper mapper) {
        this.resourceTypes = Objects.requireNonNull(resourceTypes);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public boolean shouldHandleRequest(short apiVersion) {
        return (short) 1 <= apiVersion && apiVersion <= (short) 3;
    }

    @Override
    public boolean shouldHandleResponse(short apiVersion) {
        return (short) 1 <= apiVersion && apiVersion <= (short) 3;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                          short apiVersion,
                                                          DeleteAclsRequestData request,
                                                          FilterContext filterContext,
                                                          MapperContext mapperContext) {
        log(filterContext, "request", ApiKeys.DELETE_ACLS, request);
        request.filters().stream()
                .collect(Collectors.toMap(Function.identity(),
                        r -> EntityIsolation.fromResourceTypeCode(ApiKeys.DELETE_ACLS, r.resourceTypeFilter())))
                .entrySet()
                .stream()
                .filter(e -> e.getValue().isPresent())
                .filter(e -> shouldMap(e.getValue().get())).forEach(e -> {
                    e.getKey().setResourceNameFilter(mapper.map(mapperContext, e.getValue().get(), e.getKey().resourceNameFilter()));
                });
        log(filterContext, "request result", ApiKeys.DELETE_ACLS, request);
        return filterContext.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                            short apiVersion,
                                                            Void correlatedRequestContext,
                                                            DeleteAclsResponseData response,
                                                            FilterContext filterContext,
                                                            MapperContext mapperContext) {
        log(filterContext, "response", ApiKeys.DELETE_ACLS, response);
        response.filterResults().stream().forEach(fr -> {
            var matchingAclIterator = fr.matchingAcls().iterator();
            while (matchingAclIterator.hasNext()) {
                var configResource = matchingAclIterator.next();
                EntityIsolation.fromResourceTypeCode(ApiKeys.DELETE_ACLS, configResource.resourceType())
                        .filter(entityType -> shouldMap(entityType))
                        .ifPresent(entityType -> {
                            if (mapper.isInNamespace(mapperContext, entityType, configResource.resourceName())) {
                                configResource.setResourceName(mapper.unmap(mapperContext, entityType, configResource.resourceName()));
                            }
                            else {
                                matchingAclIterator.remove();
                            }
                        });
            }
        });
        log(filterContext, "response result", ApiKeys.DELETE_ACLS, response);
        return filterContext.forwardResponse(header, response);
    }

    private boolean shouldMap(EntityIsolation.ResourceType entityType) {
        return resourceTypes.contains(entityType);
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
