/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.time.Instant;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Fortanix DSM Session.
 */
public interface Session {
    @NonNull
    String authorizationHeader();

    Instant expiration();
}
