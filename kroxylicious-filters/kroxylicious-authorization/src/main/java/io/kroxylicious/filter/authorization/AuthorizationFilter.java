/*
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A protocol filter that applies the access rules embodied in an {@link Authorization}
 * to the entities (e.g. topics and consumer groups) within the Kafka protocol.
 *
 * This class actually only implements a common mechanism, delegating the actual enforcement to {@link ApiEnforcement}
 * instances on a per-API key basis.
 */
public class AuthorizationFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AuthorizationFilter.class);

    static Map<ApiKeys, ApiEnforcement> apiEnforcement = new EnumMap<>(ApiKeys.class);

    static {
        // This filter "fails closed", rejecting all (apikey, apiversions)-combinations which is doesn't understand
        // because a new api or version could introduce a reference to some authorizable entity, like a topic,
        // which would result in information disclosure because this filter was not applying authz.
        apiEnforcement.put(ApiKeys.API_VERSIONS, new Passthrough<ApiVersionsRequestData, ApiVersionsResponseData>(0, 4) {
            @Override
            CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header, ApiVersionsResponseData response, FilterContext context,
                                                             AuthorizationFilter authorizationFilter) {
                return authorizationFilter.checkCompat(header, response, context);
            }
        });
        apiEnforcement.put(ApiKeys.SASL_HANDSHAKE, new Passthrough<>(0, 1));
        apiEnforcement.put(ApiKeys.SASL_AUTHENTICATE, new Passthrough<>(0, 2));

        apiEnforcement.put(ApiKeys.METADATA, new MetadataEnforcement());
    }

    @VisibleForTesting
    public static boolean isApiSupported(ApiKeys apiKey) {
        return apiEnforcement.containsKey(apiKey);
    }

    @VisibleForTesting
    public static boolean isApiVersionSupported(ApiKeys apiKey, short apiVersion) {
        var enforcement = apiEnforcement.get(apiKey);
        if (enforcement == null) {
            return false;
        }
        return enforcement.minSupportedVersion() <= apiVersion
                && apiVersion <= enforcement.maxSupportedVersion();
    }

    @VisibleForTesting
    public static short minSupportedApiVersion(ApiKeys apiKey) {
        var enforcement = apiEnforcement.get(apiKey);
        if (enforcement == null) {
            return -1;
        }
        return enforcement.minSupportedVersion();
    }

    @VisibleForTesting
    public static short maxSupportedApiVersion(ApiKeys apiKey) {
        var enforcement = apiEnforcement.get(apiKey);
        if (enforcement == null) {
            return -1;
        }
        return enforcement.maxSupportedVersion();
    }

    // This filter need to inspect requests, filtering out unauthorized entities before forwarding to the broker.
    // Then, when handling the response, we need to "merge" back the unauthorized entities we previously filtered out
    // so that the client observes a response with the full set of entities in the request
    // The `inflightState` stores the necessary state, keyed by correlation id.
    // TODO currently I think there's nothing which stops this growing without bound if the client is able to pipeline
    // many requests
    private final Map<Integer, InflightState<?>> inflightState;
    private short useMetadataVersion = -1;
    private final Authorizer authorizer;

    public AuthorizationFilter(Authorizer authorizer) {
        this.authorizer = authorizer;
        this.inflightState = new HashMap<>(10);
    }

    CompletionStage<AuthorizeResult> authorization(FilterContext context, List<Action> actions) {
        return authorizer.authorize(context.authenticatedSubject(), actions)
                .thenApply(authz -> {
                    LOG.info("DENY {} to {}", authz.denied(), authz.subject());
                    return authz;
                });
    }

    <R> void pushInflightState(RequestHeaderData header, InflightState<R> inflightState) {
        var existing = this.inflightState.put(header.correlationId(), inflightState);
        if (existing != null) {
            throw new IllegalStateException("Already have inflightState");
        }
    }

    <C extends InflightState<?>> C peekInflightState(int correlationId, Class<C> cClass) {
        InflightState<?> inflightState = this.inflightState.get(correlationId);
        if (inflightState == null) {
            throw new IllegalStateException("No inflightState");
        }
        return cClass.cast(inflightState);
    }

    @Nullable
    <C extends InflightState<?>> C popInflightState(ResponseHeaderData header, Class<C> cClass) {
        InflightState<?> removed = this.inflightState.remove(header.correlationId());
        return cClass.cast(removed);
    }

    <R> R popAndApplyInflightState(ResponseHeaderData header, R response) {
        var completer = popInflightState(header, InflightState.class);
        if (completer != null) {
            return (R) completer.merge(response);
        }
        else {
            return response;
        }
    }

    static IllegalStateException topicIdsNotSupported() {
        return new IllegalStateException("Topic ids not supported yet");
    }

    @Nullable
    private CompletionStage<RequestFilterResult> checkRequestApiAndVersions(ApiKeys apiKey,
                                                                            RequestHeaderData header,
                                                                            ApiMessage request,
                                                                            FilterContext context) {
        if (!isApiVersionSupported(apiKey, header.requestApiVersion())) {
            if (isApiSupported(apiKey)) {
                LOG.warn("Filter of type {} does not support {} API version {} used in request."
                        + " It supports version {} to {} (inclusive) of this API."
                        + " This error is due to a misconfigured, buggy, or possibly malicious client.",
                        getClass().getName(),
                        apiKey,
                        header.requestApiVersion(),
                        minSupportedApiVersion(apiKey),
                        maxSupportedApiVersion(apiKey));
            }
            else {
                LOG.warn("Filter of type {} does not support {} API version {} used in request."
                        + " It supports does not support version this API at all."
                        + " This error is due to a misconfigured, buggy, or possibly malicious client.",
                        getClass().getName(),
                        apiKey,
                        header.requestApiVersion());
            }
            return context.requestFilterResultBuilder()
                    .errorResponse(header, request, Errors.UNSUPPORTED_VERSION.exception())
                    .completed();
        }
        return null;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {

        var usesUnsupportedApi = checkRequestApiAndVersions(apiKey,
                header,
                request,
                context);
        if (usesUnsupportedApi != null) {
            return usesUnsupportedApi;
        }

        var enforcement = apiEnforcement.get(apiKey);
        if (enforcement != null) {
            return enforcement.onRequest(header, request, context, this);
        }
        else {
            return context.forwardRequest(header, request);
        }
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {

        var enforcement = apiEnforcement.get(apiKey);
        if (enforcement != null) {
            return enforcement.onResponse(header, response, context, this);
        }
        else {
            return context.forwardResponse(header, response);
        }
    }

    private CompletionStage<ResponseFilterResult> checkCompat(ResponseHeaderData header,
                                                              ApiVersionsResponseData response,
                                                              FilterContext context) {
        ApiVersionsResponseData.ApiVersion apiVersion = response.apiKeys().find(ApiKeys.METADATA.id);
        var minMetadataVersion = apiVersion.minVersion();
        var maxMetadataVersion = apiVersion.maxVersion();
        if (maxMetadataVersion < 4) {
            LOG.error("Filter {} requires the broker to support at least METADATA API version 4. "
                    + "The connected broker supports only {}-{}.",
                    AuthorizationFilter.class.getName(), minMetadataVersion, maxMetadataVersion);
            return context.responseFilterResultBuilder().withCloseConnection().completed();
        }
        this.useMetadataVersion = (short) Math.min(ApiKeys.METADATA.latestVersion(), maxMetadataVersion);
        adjustResponse(response);
        return context.forwardResponse(header, response);
    }

    short useMetadataVersion() {
        return useMetadataVersion;
    }

    public void adjustResponse(ApiVersionsResponseData response) {
        var toRemove = new ArrayList<ApiVersionsResponseData.ApiVersion>();
        ApiVersionsResponseData.ApiVersionCollection apiVersions = response.apiKeys();
        for (var version : apiVersions) {
            var enforcement = apiEnforcement.get(ApiKeys.forId(version.apiKey()));
            if (enforcement != null) {
                version.setMinVersion(Passthrough.asShort(Math.max(enforcement.minSupportedVersion(), version.minVersion())));
                version.setMaxVersion(Passthrough.asShort(Math.min(enforcement.maxSupportedVersion(), version.maxVersion())));
            }
            else {
                toRemove.add(version);
            }
        }
        apiVersions.removeAll(toRemove);

    }
}
