/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import com.fasterxml.jackson.annotation.JsonProperty;

public record KafTlsConfig(
                           @JsonProperty boolean insecure,
                           @JsonProperty("cafile") String caFile,
                           @JsonProperty("clientfile") String clientFile,
                           @JsonProperty("clientkeyfile") String clientKeyFile) {

}
