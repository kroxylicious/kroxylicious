/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to schedule the deletion of a KMS key.
 *
 * @param keyId id of the key to delete.
 * @param pendingWindowInDays waiting period, in days, before the key is deleted.
 * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_ScheduleKeyDeletion.html">AWS API ScheduleKeyDeletion</a>
 */
public record ScheduleKeyDeletionRequest(@JsonProperty(value = "KeyId") String keyId,
                                         @JsonProperty("PendingWindowInDays") int pendingWindowInDays) {

    /**
     * Creates a ScheduleKeyDeletionRequest.
     *
     * @param keyId id of the key to delete.
     * @param pendingWindowInDays waiting period, in days, before the key is deleted.
     */
    public ScheduleKeyDeletionRequest {
        Objects.requireNonNull(keyId);
    }
}
