/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import com.fasterxml.jackson.annotation.JsonProperty;

public record KafSaslConfig(
                            @JsonProperty String mechanism,
                            @JsonProperty String username,
                            @JsonProperty String password) {

}
