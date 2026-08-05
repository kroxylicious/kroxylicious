/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.routing.RouterDispatchHandler;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;

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
    private @Nullable Router router = null;
    private @Nullable RouterContext routerContext = null;

    DelegatingDecodePredicate() {
    }

    public void setDelegate(DecodePredicate delegate) {
        LOGGER.atDebug()
                .addKeyValue("delegate", delegate)
                .log("Setting delegate");
        this.delegate = delegate;
    }

    /**
     * Sets the router and context for interception checks.
     * The decode predicate will consult {@link Router#shouldIntercept} to determine
     * if requests need decoding for router interception.
     */
    void setRouterInterceptionDelegate(Router router, RouterContext context) {
        LOGGER.atDebug()
                .addKeyValue("router", router)
                .log("Setting router interception delegate");
        this.router = router;
        this.routerContext = context;
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
        return router != null && routerContext != null && router.shouldIntercept(apiKey, apiVersion, routerContext);
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
        if (router != null && routerContext != null) {
            // On bootstrap (unbound) connections, always decode: static pass-through sends
            // the response through the route filter chain, and route filters' onResponse must
            // see a DecodedResponseFrame. Dynamic routing also needs decoded bodies for routing
            // futures and node-ID translation.
            if (routerContext.virtualNode().isEmpty()) {
                return true;
            }
            // On bound connections: decode when the router intercepts (e.g. AlternatingRouter
            // intercepts PRODUCE) or when the response carries node IDs that need translation.
            return router.shouldIntercept(apiKey, apiVersion, routerContext)
                    || RouterDispatchHandler.NODE_ID_TRANSLATION_APIS.contains(apiKey);
        }
        return false;
    }

    @Override
    public String toString() {
        return "DelegatingDecodePredicate(" +
                "delegate=" + delegate +
                ", router=" + router +
                ')';
    }
}
