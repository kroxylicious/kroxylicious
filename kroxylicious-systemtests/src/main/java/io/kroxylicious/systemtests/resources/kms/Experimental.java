/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.systemtests.Constants;

public record Experimental(
                            @JsonProperty int resolvedAliasExpireAfterWriteSeconds,
                            @JsonProperty int resolvedAliasRefreshAfterWriteSeconds,
                            @JsonProperty int encryptionDekRefreshAfterWriteSeconds,
                            @JsonProperty int encryptionDekExpireAfterWriteSeconds) {
    public Experimental {
        Objects.requireNonNullElse(resolvedAliasExpireAfterWriteSeconds, Constants.DEFAULT_ALIAS_EXPIRATION_AFTER_SECONDS);
        Objects.requireNonNullElse(resolvedAliasRefreshAfterWriteSeconds, Constants.DEFAULT_ALIAS_REFRESH_AFTER_SECONDS);
        Objects.requireNonNullElse(encryptionDekRefreshAfterWriteSeconds, Constants.DEFAULT_DEK_REFRESH_AFTER_SECONDS);
        Objects.requireNonNullElse(encryptionDekExpireAfterWriteSeconds, Constants.DEFAULT_DEK_EXPIRATION_AFTER_SECONDS);
    }
}
