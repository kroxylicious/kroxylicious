/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.client;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A single routing rule: matches a required {@code hasClientId} to a required {@code useRoute}.
 *
 * @param hasClientId required value for the connection's pinned {@code client.id}; this can match
 *                    before, or without, authentication, since {@code client.id} is read from the
 *                    request header. A client-supplied, unverified value.
 * @param useRoute the route name, declared in the router's {@code routes} block, to forward to when
 *                 the rule matches.
 */
public record Rule(@Nullable String hasClientId,
                   @Nullable String useRoute) {
}
