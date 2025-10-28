/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config.auth;

import java.net.URI;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ManagedIdentityConfigTest {

    public static final String TARGET_RESOURCE = "https://example.com/";

    @Test
    void minimumAuthConfiguration() {
        ManagedIdentityConfig managedIdentityConfig = new ManagedIdentityConfig(TARGET_RESOURCE, null);
        assertThat(managedIdentityConfig.targetResource()).isEqualTo(TARGET_RESOURCE);
        assertThat(managedIdentityConfig.identityServiceURL()).isEqualTo(URI.create("http://169.254.169.254"));
    }

    @Test
    void overrideEndpoint() {
        ManagedIdentityConfig managedIdentityConfig = new ManagedIdentityConfig(TARGET_RESOURCE, URI.create("http://example.com"));
        assertThat(managedIdentityConfig.identityServiceURL()).isEqualTo(URI.create("http://example.com"));
    }
}
