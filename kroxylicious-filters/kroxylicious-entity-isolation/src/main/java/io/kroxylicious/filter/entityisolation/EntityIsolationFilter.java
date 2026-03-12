/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.entityisolation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
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

        // Handles version negotiation
        pm.put(ApiKeys.API_VERSIONS, new ApiVersionsHandler());
        // The following processors require special code and are handwritten.
        pm.put(ApiKeys.FIND_COORDINATOR, new FindCoordinatorEntityIsolationProcessor(resourceTypes::contains, wrappedMapper));
        pm.put(ApiKeys.DELETE_ACLS, new DeleteAclsEntityIsolationProcessor(resourceTypes::contains, wrappedMapper));
        pm.put(ApiKeys.LIST_TRANSACTIONS, new ListTransactionsEntityIsolationProcessor(resourceTypes::contains, wrappedMapper));
        pm.put(ApiKeys.DESCRIBE_ACLS, new DescribeAclsEntityIsolationProcessor(resourceTypes::contains, wrappedMapper));

        // Add the generated processors, excluding the special cases.
        createProcessorMap(resourceTypes::contains, wrappedMapper).forEach(pm::putIfAbsent);
        this.processorMap = Map.copyOf(pm);
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return Optional.ofNullable(processorMap.get(apiKey))
                .map(eip -> eip.shouldHandleRequest(apiVersion) || eip.versionIsOutOfRange(apiVersion))
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
                    if (eip.versionIsOutOfRange(apiVersion)) {
                        // fail closed
                        logFailClosed(filterContext, apiKey, apiVersion, eip.minSupportedVersion(), eip.maxSupportedVersion());
                        return filterContext.requestFilterResultBuilder()
                                .errorResponse(header, request, Errors.UNSUPPORTED_VERSION.exception())
                                .withCloseConnection()
                                .completed();
                    }
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

    private static void log(FilterContext context, String description, ApiKeys key, ApiMessage message) {
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

    private static void logFailClosed(FilterContext context, ApiKeys key, short version, short minVersion, short maxVersion) {
        LOGGER.atWarn()
                .addArgument(context::sessionId)
                .addArgument(context::authenticatedSubject)
                .addArgument(key)
                .addArgument(version)
                .addArgument(minVersion)
                .addArgument(maxVersion)
                .setMessage("{} for {}: {} (version {}) falls outside range {}...{} known to this filter. closing connection.")
                .log();
    }

    private class ApiVersionsHandler implements EntityIsolationProcessor<ApiVersionsRequestData, ApiVersionsResponseData, Void> {

        @Override
        public short minSupportedVersion() {
            return ApiKeys.API_VERSIONS.oldestVersion();
        }

        @Override
        public short maxSupportedVersion() {
            return ApiKeys.API_VERSIONS.latestVersion();
        }

        @Override
        public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                                short apiVersion,
                                                                Void correlatedRequestContext,
                                                                ApiVersionsResponseData response,
                                                                FilterContext filterContext,
                                                                MapperContext mapperContext) {
            adjustResponse(response);
            return filterContext.forwardResponse(header, response);
        }

        private void adjustResponse(ApiVersionsResponseData response) {
            var toRemove = new ArrayList<ApiVersionsResponseData.ApiVersion>();
            ApiVersionsResponseData.ApiVersionCollection apiVersions = response.apiKeys();
            for (var version : apiVersions) {
                var processor = processorMap.get(ApiKeys.forId(version.apiKey()));
                if (processor != null) {
                    version.setMinVersion(asShort(Math.max(processor.minSupportedVersion(), version.minVersion())));
                    version.setMaxVersion(asShort(Math.min(processor.maxSupportedVersion(), version.maxVersion())));
                }
                else {
                    toRemove.add(version);
                }
            }
            apiVersions.removeAll(toRemove);
        }

        static short asShort(int version) {
            if (version < 0) {
                throw new IllegalArgumentException("version cannot be negative");
            }
            short shortVersion = (short) version;
            if (shortVersion != version) {
                throw new IllegalArgumentException("version cannot be represented as a short");
            }
            return shortVersion;
        }

    }
}
