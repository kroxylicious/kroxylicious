/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.net.InetSocketAddress;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.client.KubernetesClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class WebhookMainTest {

    @Mock
    private WebhookServer server;

    @Mock
    private SidecarConfigResolver configResolver;

    @Mock
    private KubernetesClient kubeClient;

    // --- start/stop lifecycle tests ---

    @Test
    void startCallsServerStart() {
        WebhookMain main = new WebhookMain(server, configResolver, kubeClient);
        main.start();
        verify(server).start();
    }

    @Test
    void stopClosesAllComponents() {
        WebhookMain main = new WebhookMain(server, configResolver, kubeClient);
        main.stop();

        InOrder order = inOrder(server, configResolver, kubeClient);
        order.verify(server).close();
        order.verify(configResolver).close();
        order.verify(kubeClient).close();
    }

    @Test
    void stopClosesRemainingWhenServerThrows() {
        doThrow(new RuntimeException("server error")).when(server).close();

        WebhookMain main = new WebhookMain(server, configResolver, kubeClient);
        main.stop();

        verify(configResolver).close();
        verify(kubeClient).close();
    }

    @Test
    void stopClosesRemainingWhenResolverThrows() {
        doThrow(new RuntimeException("resolver error")).when(configResolver).close();

        WebhookMain main = new WebhookMain(server, configResolver, kubeClient);
        main.stop();

        verify(server).close();
        verify(kubeClient).close();
    }

    @Test
    void stopClosesRemainingWhenAllThrow() {
        doThrow(new RuntimeException("server error")).when(server).close();
        doThrow(new RuntimeException("resolver error")).when(configResolver).close();
        doThrow(new RuntimeException("client error")).when(kubeClient).close();

        WebhookMain main = new WebhookMain(server, configResolver, kubeClient);
        assertThatCode(main::stop).doesNotThrowAnyException();
    }

    // --- parseBindAddress tests ---

    @Test
    void parseBindAddressUsesDefault() {
        InetSocketAddress addr = WebhookMain.parseBindAddress(Map.of());

        assertThat(addr.getHostString()).isEqualTo("0.0.0.0");
        assertThat(addr.getPort()).isEqualTo(8443);
    }

    @Test
    void parseBindAddressUsesProvidedValue() {
        InetSocketAddress addr = WebhookMain.parseBindAddress(
                Map.of("BIND_ADDRESS", "127.0.0.1:9443"));

        assertThat(addr.getHostString()).isEqualTo("127.0.0.1");
        assertThat(addr.getPort()).isEqualTo(9443);
    }

    // --- requiredEnv tests ---

    @Test
    void requiredEnvThrowsWhenMissing() {
        assertThatThrownBy(() -> WebhookMain.requiredEnv(Map.of(), "KROXYLICIOUS_IMAGE"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("KROXYLICIOUS_IMAGE");
    }

    @Test
    void requiredEnvThrowsWhenBlank() {
        assertThatThrownBy(() -> WebhookMain.requiredEnv(
                Map.of("KROXYLICIOUS_IMAGE", "  "), "KROXYLICIOUS_IMAGE"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("KROXYLICIOUS_IMAGE");
    }

    @Test
    void requiredEnvReturnsValue() {
        String value = WebhookMain.requiredEnv(
                Map.of("KROXYLICIOUS_IMAGE", "my-image:latest"), "KROXYLICIOUS_IMAGE");

        assertThat(value).isEqualTo("my-image:latest");
    }

    // --- create() environment variable tests ---

    @Test
    @ClearEnvironmentVariable(key = "KROXYLICIOUS_IMAGE")
    void createThrowsWhenKroxyliciousImageMissing() {
        assertThatThrownBy(WebhookMain::create)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("KROXYLICIOUS_IMAGE");
    }

    @Test
    @SetEnvironmentVariable(key = "KROXYLICIOUS_IMAGE", value = "  ")
    void createThrowsWhenKroxyliciousImageBlank() {
        assertThatThrownBy(WebhookMain::create)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("KROXYLICIOUS_IMAGE");
    }
}
