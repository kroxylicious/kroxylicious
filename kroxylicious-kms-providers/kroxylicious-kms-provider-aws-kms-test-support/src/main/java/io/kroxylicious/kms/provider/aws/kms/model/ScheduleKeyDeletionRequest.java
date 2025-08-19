/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ScheduleKeyDeletionRequest(@JsonProperty(value = "KeyId") String keyId,
                                         @JsonProperty("PendingWindowInDays") int pendingWindowInDays) {

    public ScheduleKeyDeletionRequest {
        Objects.requireNonNull(keyId);
    }
}
