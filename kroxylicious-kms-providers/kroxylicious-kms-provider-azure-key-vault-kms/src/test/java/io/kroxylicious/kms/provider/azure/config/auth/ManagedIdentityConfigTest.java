/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config.auth;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ManagedIdentityConfigTest {

    public static final String TARGET_RESOURCE = "https://example.com/";

    @Test
    void minimumAuthConfiguration() {
        ManagedIdentityConfig managedIdentityConfig = new ManagedIdentityConfig(TARGET_RESOURCE, null, null);
        assertThat(managedIdentityConfig.targetResource()).isEqualTo(TARGET_RESOURCE);
        assertThat(managedIdentityConfig.identityServiceURL()).isEqualTo("http://169.254.169.254");
    }

    @Test
    void overrideHost() {
        ManagedIdentityConfig managedIdentityConfig = new ManagedIdentityConfig(TARGET_RESOURCE, "example.com", null);
        assertThat(managedIdentityConfig.identityServiceURL()).isEqualTo("http://example.com");
    }

    @Test
    void overridePort() {
        ManagedIdentityConfig managedIdentityConfig = new ManagedIdentityConfig(TARGET_RESOURCE, null, 80);
        assertThat(managedIdentityConfig.identityServiceURL()).isEqualTo("http://169.254.169.254:80");
    }
}
