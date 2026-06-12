/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.GetKeyResponse;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Paginated response from CipherTrust Manager GET /api/v1/vault/keys2 endpoint.
 *
 * @param skip number of records skipped
 * @param limit maximum number of records returned
 * @param total total number of matching records
 * @param resources list of keys (null when total is 0)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GetKeysResponse(
                              @JsonProperty("skip") int skip,
                              @JsonProperty("limit") int limit,
                              @JsonProperty("total") int total,
                              @JsonProperty("resources") @Nullable List<GetKeyResponse> resources) {}
