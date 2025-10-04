/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import io.kroxylicious.authorizer.service.Operation;

public enum FakeClusterResource implements Operation<FakeClusterResource> {
    CONNECT
}
