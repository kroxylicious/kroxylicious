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

    public SaslDecodePredicate(boolean handleSasl) {
        this.handleSasl = handleSasl;
    }

    public void setDelegate(DecodePredicate delegate) {
        LOGGER.debug("Setting delegate {}", delegate);
        this.delegate = delegate;
    }

    @Override
    public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
        boolean result;
        if (apiKey == ApiKeys.API_VERSIONS) {
            // TODO For now let's assume we need to always decode this, since the NetHandler
            // currently does this. At some point we'll need a way to figure out the mutual intersection
            // of api versions over all backend clusters plus the proxy itself.
            result = true;
        }
        else if (apiKey == ApiKeys.SASL_HANDSHAKE
                || apiKey == ApiKeys.SASL_AUTHENTICATE) {
            result = handleSasl;
        }
        else {
            result = delegate == null ? true : delegate.shouldDecodeRequest(apiKey, apiVersion);
        }
        return result;
    }

    @Override
    public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
        return delegate == null ? true : delegate.shouldDecodeResponse(apiKey, apiVersion);
    }

    @Override
    public String toString() {
        return "MyDecodePredicate(" +
                "handleSasl=" + handleSasl +
                ", delegate=" + delegate +
                ')';
    }
}
