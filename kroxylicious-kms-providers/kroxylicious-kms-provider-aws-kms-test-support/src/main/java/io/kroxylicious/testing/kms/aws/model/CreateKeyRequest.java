/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A request to create a KMS key.
 *
 * @param description description of the key.
 * @see <a href="https://docs.aws.amazon.com/kms/latest/APIReference/API_CreateKey.html">AWS API CreateKey</a>
 */
public record CreateKeyRequest(@JsonProperty("description") String description) {}
