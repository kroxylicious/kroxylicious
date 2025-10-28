/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config.auth;

import java.net.URI;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ManagedIdentityCredentialsConfigTest {

    public static final String TARGET_RESOURCE = "https://example.com/";

    @Test
    void minimumAuthConfiguration() {
        ManagedIdentityCredentialsConfig managedIdentityCredentialsConfig = new ManagedIdentityCredentialsConfig(TARGET_RESOURCE, null);
        assertThat(managedIdentityCredentialsConfig.targetResource()).isEqualTo(TARGET_RESOURCE);
        assertThat(managedIdentityCredentialsConfig.identityServiceURL()).isEqualTo(URI.create("http://169.254.169.254"));
    }

    @Test
    void overrideEndpoint() {
        ManagedIdentityCredentialsConfig managedIdentityCredentialsConfig = new ManagedIdentityCredentialsConfig(TARGET_RESOURCE, URI.create("http://example.com"));
        assertThat(managedIdentityCredentialsConfig.identityServiceURL()).isEqualTo(URI.create("http://example.com"));
    }
}
