/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum XKubeListType {
    @JsonProperty("atomic")
    ATOMIC,
    @JsonProperty("set")
    SET,
    @JsonProperty("map")
    MAP
}
