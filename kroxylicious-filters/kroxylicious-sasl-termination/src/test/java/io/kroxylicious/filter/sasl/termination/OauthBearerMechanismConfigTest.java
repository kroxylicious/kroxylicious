/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.net.URI;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OauthBearerMechanismConfigTest {

    private static final URI VALID_JWKS_URL = URI.create("https://idp.example.com/.well-known/jwks.json");

    @Test
    void shouldCreateWithRequiredFieldsOnly() {
        // When
        var config = new OauthBearerMechanismConfig(VALID_JWKS_URL, "kafka", "https://idp.example.com",
                null, null, null, null, null, null);

        // Then
        assertThat(config.jwksEndpointUrl()).isEqualTo(VALID_JWKS_URL);
        assertThat(config.expectedAudience()).isEqualTo("kafka");
        assertThat(config.expectedIssuer()).isEqualTo("https://idp.example.com");
        assertThat(config.scopeClaimName()).isNull();
        assertThat(config.subClaimName()).isNull();
        assertThat(config.jwksEndpointRefresh()).isNull();
        assertThat(config.jwksEndpointRetryBackoff()).isNull();
        assertThat(config.jwksEndpointRetryBackoffMax()).isNull();
    }

    @Test
    void shouldCreateWithAllFields() {
        // When
        var config = new OauthBearerMechanismConfig(VALID_JWKS_URL, "kafka", "https://idp.example.com",
                "scp", "subject", Duration.ofMinutes(5), Duration.ofSeconds(1), Duration.ofSeconds(30), null);

        // Then
        assertThat(config.scopeClaimName()).isEqualTo("scp");
        assertThat(config.subClaimName()).isEqualTo("subject");
        assertThat(config.jwksEndpointRefresh()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.jwksEndpointRetryBackoff()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.jwksEndpointRetryBackoffMax()).isEqualTo(Duration.ofSeconds(30));
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullJwksEndpointUrl() {
        assertThatThrownBy(() -> new OauthBearerMechanismConfig(null, "kafka", "https://idp.example.com",
                null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jwksEndpointUrl must not be null");
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullExpectedAudience() {
        assertThatThrownBy(() -> new OauthBearerMechanismConfig(VALID_JWKS_URL, null, "https://idp.example.com",
                null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedAudience must not be null or blank");
    }

    @Test
    void shouldRejectBlankExpectedAudience() {
        assertThatThrownBy(() -> new OauthBearerMechanismConfig(VALID_JWKS_URL, "  ", "https://idp.example.com",
                null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedAudience must not be null or blank");
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullExpectedIssuer() {
        assertThatThrownBy(() -> new OauthBearerMechanismConfig(VALID_JWKS_URL, "kafka", null,
                null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedIssuer must not be null or blank");
    }

    @Test
    void shouldRejectBlankExpectedIssuer() {
        assertThatThrownBy(() -> new OauthBearerMechanismConfig(VALID_JWKS_URL, "kafka", "  ",
                null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedIssuer must not be null or blank");
    }

    @Test
    void shouldReturnOauthbearerMechanismName() {
        // Given
        var config = new OauthBearerMechanismConfig(VALID_JWKS_URL, "kafka", "https://idp.example.com",
                null, null, null, null, null, null);

        // Then
        assertThat(config.mechanismName()).isEqualTo("OAUTHBEARER");
    }
}
