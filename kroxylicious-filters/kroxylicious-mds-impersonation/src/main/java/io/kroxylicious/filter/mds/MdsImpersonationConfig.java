/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for MDS impersonation using the proxy's mTLS identity.
 *
 * @param mdsUrl full HTTPS URL of the MDS impersonation endpoint
 * @param mdsTls proxy client identity and trust for MDS
 * @param requestTimeout MDS connection/request timeout; defaults to five seconds
 * @param expiryMargin time reserved before token or broker session expiry; defaults to five seconds
 */
public record MdsImpersonationConfig(URI mdsUrl, Tls mdsTls,
                                     @Nullable Duration requestTimeout, @Nullable Duration expiryMargin) {
    /** Validates the authentication boundary at startup. */
    public MdsImpersonationConfig {
        Objects.requireNonNull(mdsUrl, "mdsUrl");
        Objects.requireNonNull(mdsTls, "mdsTls");
        if (!"https".equalsIgnoreCase(mdsUrl.getScheme()) || mdsUrl.getHost() == null
                || mdsUrl.getUserInfo() != null || mdsUrl.getQuery() != null || mdsUrl.getFragment() != null) {
            throw new IllegalArgumentException("mdsUrl must be an HTTPS endpoint without credentials, query or fragment");
        }
        if (!mdsTls.definesKey() || mdsTls.credentialSupplier() != null) {
            throw new IllegalArgumentException("mdsTls.key must provide the proxy's own client certificate and private key");
        }
        if (mdsTls.trust() instanceof InsecureTls insecure && insecure.insecure()) {
            throw new IllegalArgumentException("MDS TLS verification must not be disabled");
        }
        requestTimeout = requestTimeout == null ? Duration.ofSeconds(5) : requestTimeout;
        expiryMargin = expiryMargin == null ? Duration.ofSeconds(5) : expiryMargin;
        if (requestTimeout.compareTo(Duration.ofMillis(1)) < 0 || requestTimeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("requestTimeout must be between one millisecond and one minute");
        }
        if (expiryMargin.compareTo(Duration.ofSeconds(1)) < 0 || expiryMargin.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("expiryMargin must be between one second and one minute");
        }
    }

    @Override
    public String toString() {
        return "MdsImpersonationConfig[mdsUrl=" + mdsUrl + ", mdsTls=<redacted>]";
    }
}
