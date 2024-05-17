/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata.selector;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The operator in a {@link MatchExpression}.
 */
public enum MatchOperator {
    @JsonProperty("Exists")
    EXISTS,
    @JsonProperty("DoesNotExist")
    NOT_EXISTS,
    @JsonProperty("In")
    IN,
    @JsonProperty("NotIn")
    NOT_IN
}
