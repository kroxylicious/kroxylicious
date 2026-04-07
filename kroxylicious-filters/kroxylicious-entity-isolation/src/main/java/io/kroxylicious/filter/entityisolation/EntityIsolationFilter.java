/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.entityisolation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
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

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;
import io.kroxylicious.filter.entityisolation.EntityNameMapper.EntityMapperException;
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

    EntityIsolationFilter(Set<EntityType> entityTypes, EntityNameMapper mapper) {
        Objects.requireNonNull(entityTypes);
        Objects.requireNonNull(mapper);

        Map<ApiKeys, EntityIsolationProcessor<? extends ApiMessage, ? extends ApiMessage, ?>> pm = new EnumMap<>(ApiKeys.class);

        // Handles version negotiation
        pm.put(ApiKeys.API_VERSIONS, new ApiVersionsHandler());
        // The following processors require special code and are handwritten.
        pm.put(ApiKeys.FIND_COORDINATOR, new FindCoordinatorEntityIsolationProcessor(entityTypes::contains, mapper));
        pm.put(ApiKeys.DELETE_ACLS, new DeleteAclsEntityIsolationProcessor(entityTypes::contains, mapper));
        pm.put(ApiKeys.LIST_TRANSACTIONS, new ListTransactionsEntityIsolationProcessor(entityTypes::contains, mapper));
        pm.put(ApiKeys.DESCRIBE_ACLS, new DescribeAclsEntityIsolationProcessor(entityTypes::contains, mapper));

        // Add the generated processors, excluding the special cases.
        createProcessorMap(entityTypes::contains, mapper).forEach(pm::putIfAbsent);
        this.processorMap = Collections.unmodifiableMap(pm);
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
        try {
            return Optional.ofNullable(processorMap.get(apiKey))
                    .map(EntityIsolationProcessor.class::cast)
                    .map(eip -> {
                        var mapperContext = buildMapperContext(filterContext);

                        if (eip.versionIsOutOfRange(apiVersion)) {
                            logUnexpectedApiVersion(filterContext, apiKey, apiVersion, eip.minSupportedVersion(), eip.maxSupportedVersion());
                            return filterContext.requestFilterResultBuilder()
                                    .errorResponse(header, request, Errors.UNSUPPORTED_VERSION.exception())
                                    .withCloseConnection()
                                    .completed();
                        }

                        log(filterContext, "request", apiKey, request);
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
        catch (EntityMapperException e) {
            logMappingException(filterContext, apiKey, apiVersion, e);
            return filterContext.requestFilterResultBuilder()
                    .errorResponse(header, request, Errors.UNKNOWN_SERVER_ERROR.exception())
                    .withCloseConnection()
                    .completed();
        }

    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext filterContext) {
        try {
            return Optional.ofNullable(processorMap.get(apiKey))
                    .map(EntityIsolationProcessor.class::cast)
                    .map(eip -> {
                        log(filterContext, "response", apiKey, response);
                        var mapperContext = buildMapperContext(filterContext);
                        var crc = correlatedRequestContext.remove(header.correlationId());
                        return ((CompletionStage<ResponseFilterResult>) eip.onResponse(header, apiVersion, crc, response, filterContext, mapperContext))
                                .thenApply(rfr -> {
                                    log(filterContext, "response result", apiKey, response);
                                    return rfr;
                                });
                    })
                    .orElse(filterContext.forwardResponse(header, response));
        }
        catch (EntityMapperException e) {
            logMappingException(filterContext, apiKey, apiVersion, e);
            return filterContext.responseFilterResultBuilder()
                    .withCloseConnection()
                    .completed();
        }
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

    private static void log(FilterContext context, String description, ApiKeys apiKey, ApiMessage message) {
        boolean mayContainSecret = message instanceof DescribeConfigsResponseData;
        LOGGER.atDebug()
                .addKeyValue(EntityIsolationLoggingKeys.SESSION_ID, context.sessionId())
                .addKeyValue(EntityIsolationLoggingKeys.SUBJECT, context::authenticatedSubject)
                .addKeyValue(EntityIsolationLoggingKeys.API_KEY, apiKey)
                .addKeyValue(EntityIsolationLoggingKeys.MESSAGE, mayContainSecret ? "<redacted>" : message)
                .log(description);
    }

    private static void logUnexpectedApiVersion(FilterContext context, ApiKeys apiKey, short apiVersion, short minVersion, short maxVersion) {
        LOGGER.atWarn()
                .addKeyValue(EntityIsolationLoggingKeys.SESSION_ID, context.sessionId())
                .addKeyValue(EntityIsolationLoggingKeys.SUBJECT, context::authenticatedSubject)
                .addKeyValue(EntityIsolationLoggingKeys.API_KEY, apiKey)
                .addKeyValue(EntityIsolationLoggingKeys.API_VERSION, apiVersion)
                .addKeyValue(EntityIsolationLoggingKeys.MIN_VERSION, minVersion)
                .addKeyValue(EntityIsolationLoggingKeys.MAX_VERSION, maxVersion)
                .log("API version falls outside range known to this filter, closing connection");
    }

    private static void logMappingException(FilterContext context, ApiKeys apiKey, short apiVersion, Throwable cause) {
        LOGGER.atWarn()
                .addKeyValue(EntityIsolationLoggingKeys.SESSION_ID, context.sessionId())
                .addKeyValue(EntityIsolationLoggingKeys.SUBJECT, context::authenticatedSubject)
                .addKeyValue(EntityIsolationLoggingKeys.API_KEY, apiKey)
                .addKeyValue(EntityIsolationLoggingKeys.API_VERSION, apiVersion)
                .addKeyValue(EntityIsolationLoggingKeys.ERROR, cause.getMessage())
                .setCause(LOGGER.isDebugEnabled() ? cause : null)
                .log("Operation failed, closing connection" + (LOGGER.isDebugEnabled() ? "" : ", raise log level to DEBUG for stacktrace"));
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
                var key = ApiKeys.forId(version.apiKey());
                var processor = processorMap.get(key);
                if (processor != null) {
                    // Produce is special-cased owing to https://github.com/kroxylicious/kroxylicious/pull/2851 / KAFKA-18659
                    var min = asShort(key == ApiKeys.PRODUCE ? 0 : Math.max(processor.minSupportedVersion(), version.minVersion()));
                    var max = asShort(Math.min(processor.maxSupportedVersion(), version.maxVersion()));
                    version.setMinVersion(min);
                    version.setMaxVersion(max);
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
