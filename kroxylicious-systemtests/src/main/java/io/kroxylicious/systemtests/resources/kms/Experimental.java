/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Experimental(
                           @JsonProperty int resolvedAliasExpireAfterWriteSeconds,
                           @JsonProperty int resolvedAliasRefreshAfterWriteSeconds,
                           @JsonProperty int encryptionDekRefreshAfterWriteSeconds,
                           @JsonProperty int encryptionDekExpireAfterWriteSeconds) {

}
