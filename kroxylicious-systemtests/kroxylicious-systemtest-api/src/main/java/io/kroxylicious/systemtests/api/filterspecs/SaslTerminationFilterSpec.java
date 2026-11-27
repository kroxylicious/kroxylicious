/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.filterspecs;

import java.util.Objects;

import io.kroxylicious.systemtests.api.FilterSpec;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * {@link FilterSpec} for the SASL-termination filter.
 * Sealed hierarchy with variants for each supported SASL mechanism family.
 */
public sealed interface SaslTerminationFilterSpec extends FilterSpec {

    /**
     * SCRAM-based SASL termination.
     * The fixture translates secret references into the appropriate deployment artifacts
     * (Kubernetes Secrets, local files, etc.).
     *
     * @param mechanism          the SCRAM mechanism name (e.g. "SCRAM-SHA-256", "SCRAM-SHA-512")
     * @param keystoreSecretName the name of the secret containing the SCRAM credential keystore
     * @param keystoreKey        the data key within the keystore secret
     * @param passwordSecretName the name of the secret containing the keystore password
     * @param passwordKey        the data key within the password secret
     */
    record Scram(@NonNull String mechanism,
                 @NonNull String keystoreSecretName,
                 @NonNull String keystoreKey,
                 @NonNull String passwordSecretName,
                 @NonNull String passwordKey)
            implements SaslTerminationFilterSpec {

        public Scram {
            Objects.requireNonNull(mechanism, "mechanism must not be null");
            Objects.requireNonNull(keystoreSecretName, "keystoreSecretName must not be null");
            Objects.requireNonNull(keystoreKey, "keystoreKey must not be null");
            Objects.requireNonNull(passwordSecretName, "passwordSecretName must not be null");
            Objects.requireNonNull(passwordKey, "passwordKey must not be null");
        }
    }

    /**
     * OAUTHBEARER-based SASL termination.
     *
     * @param jwksEndpointUrl  the JWKS endpoint URL for token validation
     * @param expectedAudience the expected JWT audience claim
     * @param expectedIssuer   the expected JWT issuer claim
     */
    record OAuthBearer(@NonNull String jwksEndpointUrl,
                       @NonNull String expectedAudience,
                       @NonNull String expectedIssuer)
            implements SaslTerminationFilterSpec {

        public OAuthBearer {
            Objects.requireNonNull(jwksEndpointUrl, "jwksEndpointUrl must not be null");
            Objects.requireNonNull(expectedAudience, "expectedAudience must not be null");
            Objects.requireNonNull(expectedIssuer, "expectedIssuer must not be null");
        }
    }
}