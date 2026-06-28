/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

/**
 * This policy determines what the Filter should do with records when we cannot
 * resolve a Key for that record.
 */
public enum UnresolvedKeyPolicy {
    /**
     * Passthrough the record unencrypted. This is the default policy, it allows unencrypted and encrypted topics to be handled by
     * the same Filter.
     */
    PASSTHROUGH_UNENCRYPTED,
    /**
     * Reject the record. This will cause the entire ProductRequest to be rejected and an appropriate error message will be returned
     * to the client. This is a safer policy if you know that all traffic sent to the virtual cluster should be encrypted.
     */
    REJECT
}
