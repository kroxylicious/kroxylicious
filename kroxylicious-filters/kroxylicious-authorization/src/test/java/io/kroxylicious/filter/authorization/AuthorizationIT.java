/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import io.kroxylicious.proxy.BaseIT;

public class AuthorizationIT extends BaseIT {

    // spin a cluster with Users:
    // Alice directly authorized for operation
    // Bob indirectly authorized for operation (by implication)
    // Eve not authorized for operation
    // do some prep (e.g. create a topic T, create a group G)
    // Make a non-proxied request for T as each of Alice, Bob and Eve
    // Tear down the cluster
    // spin a non-proxied cluster with users authorised the same way
    // do the same prep (e.g. create a topic T, create a group G) (non proxied)
    // Make a non-proxied request for T as each of Alice, Bob and Eve
    // Assert that the responses are ==
}
