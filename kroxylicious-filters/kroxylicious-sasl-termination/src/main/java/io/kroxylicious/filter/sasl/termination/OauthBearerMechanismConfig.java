/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.net.URI;
import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the {@code OAUTHBEARER} mechanism.
 * <p>
 * Specifies the JWKS endpoint and optional JWT validation settings for
 * OAuth 2.0 bearer token authentication.
 * </p>
 *
 * <h2>Example Configuration</h2>
 * <pre>{@code
 *   mechanism: OAUTHBEARER
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
 * @param jwksEndpointRefresh JWKS endpoint refresh interval
 * @param jwksEndpointRetryBackoff initial retry backoff
 * @param jwksEndpointRetryBackoffMax maximum retry backoff
 */
@JsonIgnoreProperties("mechanism")
public record OauthBearerMechanismConfig(
                                         @JsonProperty(required = true) URI jwksEndpointUrl,
                                         @JsonProperty(required = true) String expectedAudience,
                                         @JsonProperty(required = true) String expectedIssuer,
                                         @Nullable String scopeClaimName,
                                         @Nullable String subClaimName,
                                         @Nullable Duration jwksEndpointRefresh,
                                         @Nullable Duration jwksEndpointRetryBackoff,
                                         @Nullable Duration jwksEndpointRetryBackoffMax)
        implements MechanismConfig {

    public static final String MECHANISM_NAME = "OAUTHBEARER";

    /** Validates that required fields are present and non-blank. */
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

    @Override
    public String mechanismName() {
        return MECHANISM_NAME;
    }
}
