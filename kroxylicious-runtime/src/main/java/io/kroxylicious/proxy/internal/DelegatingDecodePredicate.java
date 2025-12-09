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
 *     <li>But it doesn't know, until it's got to the invoking the {@link io.kroxylicious.proxy.filter.NetFilter NetFilter}
 *     impl and that having called back on the {@link io.kroxylicious.proxy.filter.NetFilter.NetFilterContext NetFilterContext},
 *     what protocol filters are to be used.</li>
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

    DelegatingDecodePredicate() {
    }

    public void setDelegate(DecodePredicate delegate) {
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
        return delegate.shouldDecodeRequest(apiKey, apiVersion);
    }

    @Override
    public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
        return delegate == null || delegate.shouldDecodeResponse(apiKey, apiVersion);
    }

    @Override
    public String toString() {
        return "SaslDecodePredicate(" +
                ", delegate=" + delegate +
                ')';
    }
}
