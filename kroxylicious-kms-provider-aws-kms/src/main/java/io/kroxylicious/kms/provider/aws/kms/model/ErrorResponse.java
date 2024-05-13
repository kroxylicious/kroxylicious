/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ErrorResponse(@JsonProperty(value = "__type") String type,
                            @JsonProperty(value = "message") String message) {
    @Override
    public String toString() {
        return "ErrorResponse{" +
                "type='" + type + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
