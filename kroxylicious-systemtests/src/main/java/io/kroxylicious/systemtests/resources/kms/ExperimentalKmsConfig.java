/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ExperimentalKmsConfig(
                                    @JsonProperty long resolvedAliasExpireAfterWriteSeconds,
                                    @JsonProperty long resolvedAliasRefreshAfterWriteSeconds,
                                    @JsonProperty long encryptionDekRefreshAfterWriteSeconds,
                                    @JsonProperty long encryptionDekExpireAfterWriteSeconds) {

}
