/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.certificate.CertificateGenerator;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.integration.server.MockServer;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises real proxy TLS, subject mapping, filter discovery and Kafka wire exchanges. */
class MdsProxyTest {
    @TempDir
    Path directory;

    @Test
    void mtlsClientReceivesKafkaResponseAfterMdsAndUpstreamSasl() throws Exception {
        // Given
        try (var mds = new TestMdsServer(directory)) {
            var clientKeys = CertificateGenerator.generateRsaKeyPair();
            var certificate = CertificateGenerator.generateSelfSignedX509Certificate(clientKeys);
            var clientIdentity = TestMdsServer.writeIdentity(directory, "client", clientKeys, certificate);
            var serverSsl = SslContextBuilder.forServer(directory.resolve("mds.crt").toFile(), directory.resolve("mds.key").toFile()).build();
            try (var broker = MockServer.startOnRandomPort(null, serverSsl)) {
                broker.addMockResponseForApiKey(new ResponsePayload(ApiKeys.SASL_HANDSHAKE, (short) 1,
                        new SaslHandshakeResponseData().setMechanisms(List.of("OAUTHBEARER"))));
                broker.addMockResponseForApiKey(new ResponsePayload(ApiKeys.SASL_AUTHENTICATE, (short) 1, new SaslAuthenticateResponseData()));
                var versions = new ApiVersionsResponseData.ApiVersionCollection();
                for (ApiKeys key : ApiKeys.values()) {
                    if (key.oldestVersion() >= 0) {
                        versions.add(new ApiVersionsResponseData.ApiVersion().setApiKey(key.id).setMinVersion(key.oldestVersion()).setMaxVersion(key.latestVersion()));
                    }
                }
                broker.addMockResponse(new org.hamcrest.CustomTypeSafeMatcher<Request>("initial version discovery") {
                    @Override
                    protected boolean matchesSafely(Request candidate) {
                        return candidate.apiKeys() == ApiKeys.API_VERSIONS && candidate.apiVersion() == 0;
                    }
                }, new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, new ApiVersionsResponseData().setApiKeys(versions)));
                broker.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData().setApiKeys(versions)));
                var parser = new ConfigParser();
                var config = parser.parseConfiguration(MdsProxyTestConfig.configuration(directory, broker.port(), mds));
                try (var proxy = new KafkaProxy(parser, config, Features.defaultFeatures())) {
                    var shutdown = proxy.startup();
                    var address = proxy.getBootstrapAddress("confluent", "clients");
                    var clientSsl = SslContextBuilder.forClient().trustManager(directory.resolve("mds.crt").toFile())
                            .keyManager(Path.of(clientIdentity.certificateFile()).toFile(), Path.of(clientIdentity.privateKeyFile()).toFile()).build();
                    try (var client = new KafkaClient("localhost", address.port(), clientSsl)) {
                        // When
                        var response = client.get(new Request(ApiKeys.API_VERSIONS, (short) 3, "unchanged-mtls-client",
                                new ApiVersionsRequestData().setClientSoftwareName("test").setClientSoftwareVersion("1"))).get(15, TimeUnit.SECONDS);

                        // Then
                        assertThat(shutdown).isNotDone();
                        assertThat(((ApiVersionsResponseData) response.payload().message()).errorCode()).isZero();
                        assertThat(broker.getReceivedRequests()).extracting(Request::apiKeys)
                                .containsExactly(ApiKeys.API_VERSIONS, ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE, ApiKeys.API_VERSIONS);
                        var auth = (SaslAuthenticateRequestData) broker.getReceivedRequests().get(2).message();
                        assertThat(new String(auth.authBytes(), StandardCharsets.UTF_8)).startsWith("n,,\u0001auth=Bearer ");
                        assertThat(mds.requestBody).contains("\"targetPrincipalName\":\"alice\"");
                        assertThat(mds.peerCertificate).isEqualTo(mds.proxyCertificate).isNotEqualTo(certificate);
                    }
                }
            }
        }
    }

}
