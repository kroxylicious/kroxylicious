/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The response from a request to schedule the deletion of a KMS key.
 *
 * @param keyState current state of the key.
 * @param pendingWindowInDays waiting period, in days, before the key is deleted.
 * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_ScheduleKeyDeletion.html">AWS API ScheduleKeyDeletion</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ScheduleKeyDeletionResponse(@JsonProperty(value = "KeyState") String keyState,
                                          @JsonProperty(value = "PendingWindowInDays") int pendingWindowInDays) {

    /**
     * Creates a ScheduleKeyDeletionResponse.
     *
     * @param keyState current state of the key.
     * @param pendingWindowInDays waiting period, in days, before the key is deleted.
     */
    public ScheduleKeyDeletionResponse {
        Objects.requireNonNull(keyState);
    }
}
