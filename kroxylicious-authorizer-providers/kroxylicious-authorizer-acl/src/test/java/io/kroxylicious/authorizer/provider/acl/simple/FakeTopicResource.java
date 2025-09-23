/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl.simple;

import io.kroxylicious.authorizer.service.Operation;

public enum FakeTopicResource implements Operation<FakeTopicResource> {
    DESCRIBE,
    CREATE,
    WRITE,
    ALTER,
    READ
}
