/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.entityisolation;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.entityisolation.EntityIsolation.ResourceType;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static io.kroxylicious.filter.entityisolation.EntityIsolationProcessorMapFactory.createProcessorMap;

/**
* Entity isolation filter.
*/
class EntityIsolationFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntityIsolationFilter.class);

    private final Map<ApiKeys, ? extends EntityIsolationProcessor<? extends ApiMessage, ? extends ApiMessage, ?>> processorMap;
    private final Map<Integer, Object> correlatedRequestContext = new HashMap<>();

    EntityIsolationFilter(Set<ResourceType> resourceTypes, EntityNameMapper mapper) {
        Objects.requireNonNull(resourceTypes);
        Objects.requireNonNull(mapper);
        var wrappedMapper = new EmptyResourceNameHandlingMapper(Objects.requireNonNull(mapper));

        Map<ApiKeys, EntityIsolationProcessor<? extends ApiMessage, ? extends ApiMessage, ?>> pm = new HashMap<>();
        // The following processors require special code and are handwritten.
        pm.put(ApiKeys.FIND_COORDINATOR, new FindCoordinatorEntityIsolationProcessor(resourceTypes::contains, wrappedMapper));
        pm.put(ApiKeys.DELETE_ACLS, new DeleteAclsEntityIsolationProcessor(resourceTypes::contains, wrappedMapper));
        pm.put(ApiKeys.LIST_TRANSACTIONS, new ListTransactionsEntityIsolationProcessor(resourceTypes::contains, wrappedMapper));
        pm.put(ApiKeys.DESCRIBE_ACLS, new DescribeAclsEntityIsolationProcessor(resourceTypes::contains, wrappedMapper));
        // Add the generated processors.
        pm.putAll(createProcessorMap(resourceTypes::contains, wrappedMapper));
        this.processorMap = Map.copyOf(pm);
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return Optional.ofNullable(processorMap.get(apiKey))
                .map(eip -> eip.shouldHandleRequest(apiVersion))
                .orElse(false);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return Optional.ofNullable(processorMap.get(apiKey))
                .map(eip -> eip.shouldHandleResponse(apiVersion))
                .orElse(false);
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext filterContext) {
        return Optional.ofNullable(processorMap.get(apiKey))
                .map(EntityIsolationProcessor.class::cast)
                .map(eip -> {
                    log(filterContext, "request", apiKey, request);
                    var mapperContext = buildMapperContext(filterContext);
                    var crc = eip.createCorrelatedRequestContext(request);
                    return ((CompletionStage<RequestFilterResult>) eip.onRequest(header, apiVersion, request, filterContext, mapperContext))
                            .thenApply(rfr -> {
                                if (!(crc == null || rfr.shortCircuitResponse() || rfr.drop() || rfr.closeConnection())) {
                                    correlatedRequestContext.put(header.correlationId(), crc);
                                }
                                log(filterContext, "request result", apiKey, request);
                                return rfr;
                            });
                })
                .orElse(filterContext.forwardRequest(header, request));

    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext filterContext) {
        return Optional.ofNullable(processorMap.get(apiKey))
                .map(EntityIsolationProcessor.class::cast)
                .map(eip -> {
                    log(filterContext, "response", apiKey, response);
                    var mapperContext = buildMapperContext(filterContext);
                    var crc = correlatedRequestContext.remove(header.correlationId());
                    return eip.onResponse(header, apiVersion, crc, response, filterContext, mapperContext)
                            .thenApply(rfr -> {
                                log(filterContext, "response result", apiKey, response);
                                return rfr;
                            });
                })
                .orElse(filterContext.forwardResponse(header, response));
    }

    @VisibleForTesting
    Map<Integer, Object> getCorrelatedRequestContextMap() {
        return correlatedRequestContext;
    }

    private static MapperContext buildMapperContext(FilterContext context) {
        return new MapperContext(context.authenticatedSubject(),
                context.clientTlsContext().orElse(null),
                context.clientSaslContext().orElse(null));
    }

    private static class EmptyResourceNameHandlingMapper implements EntityNameMapper {

        private final EntityNameMapper mapper;

        EmptyResourceNameHandlingMapper(EntityNameMapper entityNameMapper) {
            this.mapper = Objects.requireNonNull(entityNameMapper);
        }

        @Override
        public String map(MapperContext mapperContext, ResourceType resourceType, String originalName) {
            if (originalName == null || originalName.isEmpty()) {
                return originalName;
            }
            return mapper.map(mapperContext, resourceType, originalName);
        }

        @Override
        public String unmap(MapperContext mapperContext, ResourceType resourceType, String mappedName) {
            if (mappedName == null || mappedName.isEmpty()) {
                return mappedName;
            }
            return mapper.unmap(mapperContext, resourceType, mappedName);
        }

        @Override
        public boolean isInNamespace(MapperContext mapperContext, ResourceType resourceType, String mappedName) {
            if (mappedName == null || mappedName.isEmpty()) {
                return false;
            }

            return mapper.isInNamespace(mapperContext, resourceType, mappedName);
        }
    }

    public static void log(FilterContext context, String description, ApiKeys key, ApiMessage message) {
        boolean mayContainSecret = message instanceof DescribeConfigsResponseData;
        LOGGER.atDebug()
                .addArgument(context::sessionId)
                .addArgument(context::authenticatedSubject)
                .addArgument(description)
                .addArgument(key)
                .addArgument(mayContainSecret ? "<redacted>" : message)
                .setMessage("{} for {}: {} {}: {}")
                .log();
    }

}
