/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.net.URI;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the OAUTHBEARER mechanism.
 * <p>
 * Specifies the JWKS endpoint and optional JWT validation settings for
 * OAuth 2.0 bearer token authentication.
 * </p>
 *
 * <h2>Example Configuration</h2>
 * <pre>{@code
 * OAUTHBEARER:
 *   jwksEndpointUrl: https://idp.example.com/.well-known/jwks.json
 *   expectedAudience: kafka
 *   expectedIssuer: https://idp.example.com
 * }</pre>
 *
 * @param jwksEndpointUrl the JWKS endpoint URL for fetching signing keys
 * @param expectedAudience the expected audience claim (comma-separated for multiple)
 * @param expectedIssuer the expected issuer claim
 * @param scopeClaimName custom claim name for scope (default: "scope")
 * @param subClaimName custom claim name for subject (default: "sub")
 * @param jwksEndpointRefreshMs JWKS endpoint refresh interval in milliseconds
 * @param jwksEndpointRetryBackoffMs initial retry backoff in milliseconds
 * @param jwksEndpointRetryBackoffMaxMs maximum retry backoff in milliseconds
 */
public record OauthBearerMechanismConfig(
                                         @JsonProperty(required = true) URI jwksEndpointUrl,
                                         @JsonProperty(required = true) String expectedAudience,
                                         @JsonProperty(required = true) String expectedIssuer,
                                         @Nullable String scopeClaimName,
                                         @Nullable String subClaimName,
                                         @Nullable Long jwksEndpointRefreshMs,
                                         @Nullable Long jwksEndpointRetryBackoffMs,
                                         @Nullable Long jwksEndpointRetryBackoffMaxMs)
        implements MechanismConfig {

    public OauthBearerMechanismConfig {
        if (jwksEndpointUrl == null) {
            throw new IllegalArgumentException("jwksEndpointUrl must not be null");
        }
        if (expectedAudience == null || expectedAudience.isBlank()) {
            throw new IllegalArgumentException("expectedAudience must not be null or blank");
        }
        if (expectedIssuer == null || expectedIssuer.isBlank()) {
            throw new IllegalArgumentException("expectedIssuer must not be null or blank");
        }
    }
}
