/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ClientAddressRecordingFilter;
import io.kroxylicious.it.testplugins.ClientAddressRecordingFilter.Observation;
import io.kroxylicious.it.testplugins.ClientAddressRecordingFilterFactory;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.ProxyProtocolMode;
import io.kroxylicious.proxy.proxyprotocol.ClientProxyProtocolContext;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.MockServerKroxyliciousTester;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.DEFAULT_CLUSTER_DEF_NAME;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.DEFAULT_CLUSTER_TARGET;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.OS_ASSIGNED_BOOTSTRAP;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/**
 * End-to-end integration test for PROXY protocol support using a real Envoy proxy container.
 * Envoy is configured to inject PROXY protocol v2 headers on upstream connections, mirroring
 * what a production load balancer would do. This complements {@link ProxyProtocolIT}, which
 * tests the same feature using a modified client that injects raw PROXY protocol bytes directly.
 */
@ExtendWith(NettyLeakDetectorExtension.class)
@EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
class ProxyProtocolEndToEndIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyProtocolEndToEndIT.class);

    // envoyproxy/envoy is a Docker Hub image; MirrorImageNameSubstitutor (wired via
    // testcontainers.properties) automatically redirects the pull to mirror.gcr.io.
    private static final DockerImageName ENVOY_IMAGE = DockerImageName.parse(
            "envoyproxy/envoy:v1.40.2@sha256:401cf6645d36f5e599dd26eec125a0a9d21c92267bc37b702d24c327b1ed889a");
    private static final int ENVOY_LISTENER_PORT = 10000;
    private static final int ENVOY_ADMIN_PORT = 9901;

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Test
    void envoyInjectedProxyProtocolHeaderExposesClientAddress() {
        // Given
        String recorderId = "envoy-e2e";
        ClientAddressRecordingFilter.reset(recorderId);
        var recordingFilter = new NamedFilterDefinitionBuilder("client-address-recorder",
                ClientAddressRecordingFilterFactory.class.getName())
                .withConfig("recorderId", recorderId)
                .build();

        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> plainConfig(bootstrap, ProxyProtocolMode.REQUIRED)
                .addToFilterDefinitions(recordingFilter)
                .addToDefaultFilters(recordingFilter.name()))) {
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS,
                    ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsResponseData()));

            var kroxyliciousPort = HostPort.parse(tester.getBootstrapAddress()).port();
            // Make the loopback-bound kroxylicious port reachable from inside the Envoy container.
            Testcontainers.exposeHostPorts(kroxyliciousPort);

            var envoyConfig = buildEnvoyConfig("host.testcontainers.internal", kroxyliciousPort);
            try (var envoy = createEnvoyContainer(envoyConfig)) {
                envoy.start();
                var envoyContainerIp = getEnvoyContainerIp(envoy);

                // When
                sendClientRequestThroughEnvoy(tester, "localhost:" + envoy.getMappedPort(ENVOY_LISTENER_PORT));

                // Then
                var observation = ClientAddressRecordingFilter.observationFor(recorderId);

                // The PP source is the original downstream client's IP as seen by Envoy. It varies by
                // container runtime (Docker bridge gateway on Linux, libkrun VM address on macOS/Podman),
                // so we assert it differs from Envoy's own container IP rather than pinning a value.
                assertThat(observation)
                        .extracting(Observation::clientAddress, type(InetSocketAddress.class))
                        .extracting(InetSocketAddress::getHostString)
                        .as("PP source address should be the original test-client IP as seen by Envoy, not Envoy's own container IP")
                        .isNotEqualTo(envoyContainerIp);

                // The PP destination is the downstream client's destination = Envoy's listener address.
                // Envoy's listener binds on the container IP, which we can look up from the Docker API.
                // A match here proves Envoy injected a valid PP v2 header on the upstream connection.
                assertThat(observation)
                        .extracting(Observation::clientProxyProtocolContext)
                        .extracting(ClientProxyProtocolContext::destinationAddress, type(InetSocketAddress.class))
                        .extracting(InetSocketAddress::getHostString)
                        .as("PP destination address should be Envoy's container IP (its listener address)")
                        .isEqualTo(envoyContainerIp);

            }
        }
    }

    private static void sendClientRequestThroughEnvoy(MockServerKroxyliciousTester tester, String address) {
        try (var client = tester.simpleTestClient(address, false)) {
            assertThat(client.get(apiVersionsRequest())).succeedsWithin(10, TimeUnit.SECONDS);
        }
        tester.assertAllMockInteractionsInvoked();
    }

    @Nullable
    private static String getEnvoyContainerIp(GenericContainer<? extends GenericContainer<?>> envoy) {
        return envoy.getContainerInfo().getNetworkSettings().getNetworks()
                .values().iterator().next().getIpAddress();
    }

    // ---- helpers ----

    private static Request apiVersionsRequest() {
        return new Request(ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION,
                "test-client", new ApiVersionsRequestData());
    }

    private static ConfigurationBuilder plainConfig(String bootstrap, ProxyProtocolMode mode) {
        // @formatter:off
        // Note: for a test that exercises metadata discovery (Produce/Fetch), brokerAddressPattern
        // on the PortIdentifiesNode gateway would need to reference the Envoy address so that
        // clients reach the proxy exclusively through Envoy.
        return baseConfigurationBuilder()
                .addNewClusterDefinition()
                    .withName(DEFAULT_CLUSTER_DEF_NAME)
                    .withBootstrapServers(bootstrap)
                .endClusterDefinition()
                .addNewVirtualCluster()
                    .withName("demo")
                    .withTarget(DEFAULT_CLUSTER_TARGET)
                    .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP)
                            .withNewProxyProtocol()
                                .withMode(mode)
                            .endProxyProtocol()
                            .build())
                .endVirtualCluster();
        // @formatter:on
    }

    private static GenericContainer<?> createEnvoyContainer(String configContent) {
        // Override the default entrypoint (docker-entrypoint.sh) which does `chown /dev/stdout /dev/stderr`
        // — that chown fails in rootless Podman. Running the envoy binary directly avoids the issue.
        return new GenericContainer<>(ENVOY_IMAGE)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/usr/local/bin/envoy").withCmd("-c", "/etc/envoy/envoy.yaml"))
                .withCopyToContainer(Transferable.of(configContent.getBytes(StandardCharsets.UTF_8), 0644), "/etc/envoy/envoy.yaml")
                .withExposedPorts(ENVOY_LISTENER_PORT, ENVOY_ADMIN_PORT)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .waitingFor(Wait.forHttp("/ready").forPort(ENVOY_ADMIN_PORT).forStatusCode(200));
    }

    private static String buildEnvoyConfig(String upstreamHost, int upstreamPort) {
        return """
                admin:
                  address:
                    socket_address:
                      address: 0.0.0.0
                      port_value: 9901
                static_resources:
                  listeners:
                  - name: listener_0
                    address:
                      socket_address:
                        address: 0.0.0.0
                        port_value: 10000
                    filter_chains:
                    - filters:
                      - name: envoy.filters.network.tcp_proxy
                        typed_config:
                          "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
                          stat_prefix: ingress_tcp
                          cluster: kroxylicious_cluster
                  clusters:
                  - name: kroxylicious_cluster
                    connect_timeout: 10s
                    type: STRICT_DNS
                    load_assignment:
                      cluster_name: kroxylicious_cluster
                      endpoints:
                      - lb_endpoints:
                        - endpoint:
                            address:
                              socket_address:
                                address: %s
                                port_value: %d
                    transport_socket:
                      name: envoy.transport_sockets.upstream_proxy_protocol
                      typed_config:
                        "@type": type.googleapis.com/envoy.extensions.transport_sockets.proxy_protocol.v3.ProxyProtocolUpstreamTransport
                        config:
                          version: V2
                        transport_socket:
                          name: envoy.transport_sockets.raw_buffer
                          typed_config:
                            "@type": type.googleapis.com/envoy.extensions.transport_sockets.raw_buffer.v3.RawBuffer
                """.formatted(upstreamHost, upstreamPort);
    }
}
