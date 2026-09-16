/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.certificate.CertificateGenerator;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.client.KafkaClient;

import static org.assertj.core.api.Assertions.assertThat;

class MdsReauthenticationProxyTest {
    @TempDir
    Path directory;

    @Test
    void reauthenticatesWithPipelinedRequestsAndAnApplicationResponseInFlight() throws Exception {
        // Given
        try (var mds = new TestMdsServer(directory)) {
            var keys = CertificateGenerator.generateRsaKeyPair();
            var identity = TestMdsServer.writeIdentity(directory, "client", keys, CertificateGenerator.generateSelfSignedX509Certificate(keys));
            var serverTls = SslContextBuilder.forServer(directory.resolve("mds.crt").toFile(), directory.resolve("mds.key").toFile()).build();
            try (var broker = new ReauthenticationBroker(serverTls)) {
                var parser = new ConfigParser();
                var yaml = MdsProxyTestConfig.configuration(directory, broker.port(), mds)
                        .replace("filterDefinitions:", "filterDefinitions:\n  - name: delayed\n    type: DelayedTraffic")
                        .replace("filters: [mds]", "filters: [delayed, mds]");
                var config = parser.parseConfiguration(yaml);
                try (var proxy = new KafkaProxy(parser, config, Features.defaultFeatures())) {
                    var shutdown = proxy.startup();
                    var address = proxy.getBootstrapAddress("confluent", "clients");
                    var clientTls = SslContextBuilder.forClient().trustManager(directory.resolve("mds.crt").toFile())
                            .keyManager(Path.of(identity.certificateFile()).toFile(), Path.of(identity.privateKeyFile()).toFile()).build();
                    try (var client = new KafkaClient("localhost", address.port(), clientTls)) {
                        client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "warmup",
                                new ApiVersionsRequestData().setClientSoftwareName("test").setClientSoftwareVersion("1")));
                        var earlier = client.get(groups("earlier"));
                        broker.applicationPending.get(10, TimeUnit.SECONDS);

                        // When
                        var later = client.get(groups("later"));
                        var noAck = client.get(new Request(ApiKeys.PRODUCE, (short) 9, "zero-acks", new ProduceRequestData().setAcks((short) 0)));
                        var last = client.get(groups("last"));

                        broker.handshakePending.get(10, TimeUnit.SECONDS);
                        var earlierResponse = earlier.get(10, TimeUnit.SECONDS);
                        boolean heldDuringHandshake = !later.isDone() && !last.isDone();
                        var requestsDuringHandshake = List.copyOf(broker.requests);
                        broker.releaseHandshake.complete(null);
                        broker.authenticationPending.get(10, TimeUnit.SECONDS);
                        boolean heldDuringAuthentication = !later.isDone() && !last.isDone();
                        broker.releaseAuthentication.complete(null);

                        // Then
                        assertThat(shutdown).isNotDone();
                        assertThat(((ListGroupsResponseData) earlierResponse.payload().message()).groups())
                                .extracting(ListGroupsResponseData.ListedGroup::groupId).containsExactly("earlier");
                        assertThat(heldDuringHandshake).isTrue();
                        assertThat(heldDuringAuthentication).isTrue();
                        assertThat(requestsDuringHandshake).containsExactly(ApiKeys.API_VERSIONS, ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE, ApiKeys.API_VERSIONS,
                                ApiKeys.LIST_GROUPS, ApiKeys.SASL_HANDSHAKE);
                        assertThat(later).succeedsWithin(Duration.ofSeconds(10)).satisfies(response -> assertThat(
                                ((ListGroupsResponseData) response.payload().message()).groups()).extracting(ListGroupsResponseData.ListedGroup::groupId)
                                .containsExactly("later"));
                        assertThat(noAck).succeedsWithin(Duration.ofSeconds(10)).isNull();
                        assertThat(last).succeedsWithin(Duration.ofSeconds(10)).satisfies(response -> assertThat(
                                ((ListGroupsResponseData) response.payload().message()).groups()).extracting(ListGroupsResponseData.ListedGroup::groupId)
                                .containsExactly("last"));
                        assertThat(client.isOpen()).isTrue();
                        assertThat(broker.connections).hasValue(1);
                        assertThat(mds.requests).hasValue(2);
                        assertThat(broker.requests).containsExactly(ApiKeys.API_VERSIONS, ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE, ApiKeys.API_VERSIONS,
                                ApiKeys.LIST_GROUPS, ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE, ApiKeys.API_VERSIONS, ApiKeys.LIST_GROUPS,
                                ApiKeys.PRODUCE, ApiKeys.LIST_GROUPS);
                    }
                }
            }
        }
    }

    private static Request groups(String clientId) {
        return new Request(ApiKeys.LIST_GROUPS, (short) 4, clientId, new ListGroupsRequestData());
    }
}
