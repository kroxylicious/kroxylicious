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

class SaslDecodePredicate implements DecodePredicate {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslDecodePredicate.class);

    private final boolean handleSasl;
    private DecodePredicate delegate = null;

    SaslDecodePredicate(boolean handleSasl) {
        this.handleSasl = handleSasl;
    }

    public void setDelegate(DecodePredicate delegate) {
        /*
         * This delegate is ugly. The problem it is solving is:
         *
         * 1. We want the proxy to avoid deserializing requests and responses "when it doesn't have to"
         * * So when there isn't a filter which is interested in that request/response API, or API version
         * * And when the proxy infra itself doesn't need to
         * 2. With the SASL offload support the proxy itself (when configured) is interested in the SASL APIs.
         * 3. But it doesn't know, until it's got to the invoking the `NetFilter` impl and that having called
         * back on the `NetFilterContext`, what protocol filters are to be used.
         * 4. But it's the `KafkaDecodeFilter` which needs to know about decodability, and that sits in front
         * of the `KafkaProxyFrontendHandler`, so there's a cyclic dependency.
         * 5. It's easier to use this delegation pattern than it is to try to reconfigure
         * the predicate on the `KafkaDecodeFilter`.
         */
        LOGGER.debug("Setting delegate {}", delegate);
        this.delegate = delegate;
    }

    @Override
    public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
        if (apiKey == ApiKeys.API_VERSIONS) {
            // TODO For now let's assume we need to always decode this, since the NetHandler
            // currently does this. At some point we'll need a way to figure out the mutual intersection
            // of api versions over all backend clusters plus the proxy itself.
            return true;
        }
        if (delegate == null) {
            // on the first request, before the delegate is set decode everything in case a filter wants
            // to intercept it
            return true;
        }
        return isAuthenticationOffloaded(apiKey) || delegate.shouldDecodeRequest(apiKey, apiVersion);
    }

    private boolean isAuthenticationOffloaded(ApiKeys apiKey) {
        return isAuthenticationOffloadEnabled() && (apiKey == ApiKeys.SASL_HANDSHAKE || apiKey == ApiKeys.SASL_AUTHENTICATE);
    }

    public boolean isAuthenticationOffloadEnabled() {
        return handleSasl;
    }

    @Override
    public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
        return delegate == null || delegate.shouldDecodeResponse(apiKey, apiVersion);
    }

    @Override
    public String toString() {
        return "SaslDecodePredicate("
               +
               "handleSasl="
               + handleSasl
               +
               ", delegate="
               + delegate
               +
               ')';
    }
}
