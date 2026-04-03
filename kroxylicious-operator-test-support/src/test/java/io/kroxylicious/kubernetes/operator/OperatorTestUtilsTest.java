/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
class OperatorTestUtilsTest {

    // Injected by @EnableKubernetesMockClient
    KubernetesClient kubeClient;

    @Test
    void kubeClientShouldReturnNonNullClient() {
        try (var client = OperatorTestUtils.kubeClient(new KubernetesClientBuilder().withConfig(kubeClient.getConfiguration()))) {
            assertThat(client).isNotNull();
        }
    }

    @Test
    void isKubeClientAvailableShouldReturnTrueWhenServerResponds() {
        var builder = new KubernetesClientBuilder().withConfig(kubeClient.getConfiguration());
        assertThat(OperatorTestUtils.isKubeClientAvailable(builder)).isTrue();
    }

    @Test
    void isKubeClientAvailableShouldReturnFalseWhenServerIsUnreachable() {
        var builder = new KubernetesClientBuilder()
                .editOrNewConfig()
                .withMasterUrl("https://localhost:1") // nothing listening here
                .withRequestRetryBackoffLimit(0)
                .withConnectionTimeout(500)
                .endConfig();
        assertThat(OperatorTestUtils.isKubeClientAvailable(builder)).isFalse();
    }

    @Test
    void isKubeClientAvailableShouldReturnFalseWhenBuildThrows() {
        var builder = mock(KubernetesClientBuilder.class);
        when(builder.build()).thenThrow(new RuntimeException("build failed"));
        assertThat(OperatorTestUtils.isKubeClientAvailable(builder)).isFalse();
    }

    @Test
    void isKubeClientAvailableShouldReturnFalseWhenBuildReturnsNull() {
        var builder = mock(KubernetesClientBuilder.class);
        when(builder.build()).thenReturn(null);
        assertThat(OperatorTestUtils.isKubeClientAvailable(builder)).isFalse();
    }
}
