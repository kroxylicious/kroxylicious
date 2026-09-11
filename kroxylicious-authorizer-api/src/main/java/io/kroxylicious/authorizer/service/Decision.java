/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

/**
 * An authorization decision about whether a particular subject is allowed to perform a
 * specific operation on a particular resource.
 */
public enum Decision {
    /**
     * The subject is allowed to perform the operation on the resource.
     */
    ALLOW,

    /**
     * The subject is not allowed to perform the operation on the resource.
     */
    DENY;
}
