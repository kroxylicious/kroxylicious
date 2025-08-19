/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import java.util.Locale;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Encapsulates an AWS error response.
 *
 * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/CommonErrors.html">https://docs.aws.amazon.com/kms/latest/APIReference/CommonErrors.html</a>
 *
 * @param type type of error
 * @param message associated error message
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ErrorResponse(@JsonProperty(value = "__type") String type,
                            @JsonProperty(value = "message") @Nullable String message) {
    public ErrorResponse {
        Objects.requireNonNull(type);
    }

    public boolean isNotFound() {
        return (type().equalsIgnoreCase("NotFoundException") ||
                (type().equalsIgnoreCase("KMSInvalidStateException") && String.valueOf(message()).toLowerCase(Locale.ROOT).contains("is pending deletion")));
    }

    @Override
    public String toString() {
        return "ErrorResponse{" +
                "type='" + type + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
