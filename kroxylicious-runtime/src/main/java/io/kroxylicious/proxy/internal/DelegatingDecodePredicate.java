/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Set;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.codec.DecodePredicate;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * DecodePredicate that:
 * <ol>
 *     <li>Always decodes ApiVersions</li>
 *     <li>Decodes all RPCs initially, until a delegate is installed</li>
 *     <li>After the delegate is installed, use that to determine if non-ApiVersions RPCs should be decoded</li>
 * </ol>
 * The problem this class is solving is:
 * <ol>
 *     <li>We want the proxy to avoid deserializing requests and responses "when it doesn't have to".
 *     So when there isn't a filter which is interested in that request/response API, or API version
 *     And when the proxy infra itself doesn't need to.</li>
 *     <li>But it doesn't know which protocol filters are to be used until the backend connection is initiated.</li>
 *     <li>But it's the {@link io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder KafkaRequestDecoder}
 *     which needs to know about decodability, and that sits in front of the {@link KafkaProxyFrontendHandler},
 *     so there's a cyclic dependency.</li>
 *     <li>It's easier to use this delegation pattern than it is to try to reconfigure
 *      the predicate on the {@link io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder KafkaRequestDecoder}.</li>
 * </ol>
 */
class DelegatingDecodePredicate implements DecodePredicate {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingDecodePredicate.class);

    private @Nullable DecodePredicate delegate = null;
    private @Nullable Set<ApiKeys> routerRequiresDecoding = null;

    DelegatingDecodePredicate() {
    }

    public void setDelegate(DecodePredicate delegate) {
        LOGGER.atDebug()
                .addKeyValue("delegate", delegate)
                .log("Setting delegate");
        this.delegate = delegate;
    }

    /**
     * Sets the API keys for which the router requires decoded frames.
     * These are the dynamically-routed keys that need {@code onClientRequest}.
     */
    void setRouterDecodingRequirements(@Nullable Set<ApiKeys> dynamicallyRoutedKeys) {
        this.routerRequiresDecoding = dynamicallyRoutedKeys;
    }

    @Override
    public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
        if (apiKey == ApiKeys.API_VERSIONS) {
            return true;
        }
        if (delegate == null) {
            return true;
        }
        if (delegate.shouldDecodeRequest(apiKey, apiVersion)) {
            return true;
        }
        return routerRequiresDecoding != null && routerRequiresDecoding.contains(apiKey);
    }

    @Override
    public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
        if (apiKey == ApiKeys.API_VERSIONS) {
            return true;
        }
        if (delegate == null) {
            return true;
        }
        if (delegate.shouldDecodeResponse(apiKey, apiVersion)) {
            return true;
        }
        return routerRequiresDecoding != null && routerRequiresDecoding.contains(apiKey);
    }

    @Override
    public String toString() {
        return "DelegatingDecodePredicate(" +
                "delegate=" + delegate +
                ", routerRequiresDecoding=" + routerRequiresDecoding +
                ')';
    }
}
