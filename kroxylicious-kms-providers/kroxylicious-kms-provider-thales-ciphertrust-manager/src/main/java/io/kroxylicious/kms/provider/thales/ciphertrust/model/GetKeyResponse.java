/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response model for CipherTrust Manager key retrieval.
 * Only includes minimal fields needed for key resolution.
 *
 * @param id the key ID
 * @param name the key name
 * @param algorithm the key algorithm
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GetKeyResponse(
                             @JsonProperty("id") String id,
                             @JsonProperty("name") String name,
                             @JsonProperty("algorithm") String algorithm) {}
