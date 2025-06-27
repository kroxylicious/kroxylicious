/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.time.Instant;

/**
 * Fortanix DSM Session.
 */
public interface Session {
    /**
     * The complete authorization header that needs to be passed to Fortanix DSM endpoint requests that
     * require authentication.
     *
     * @return authentication header
     */
    String authorizationHeader();

    /**
     * Time when this session will expire.
     *
     * @return expiration time.
     */
    Instant expiration();

    /**
     * Causes this session to be invalidated so that it will not be used again.
     * <br/>
     * Call this method if the Fortanix DSM API returns a spontaneous authorization failure
     * (401/403 etc.). We don't normally expect to follow this path, as the {@link SessionProvider} takes
     * responsibility to refresh the session before it expires, however, events like a
     * server-side failover may result in us experience a spontaneous authorization error.
     */
    void invalidate();
}
