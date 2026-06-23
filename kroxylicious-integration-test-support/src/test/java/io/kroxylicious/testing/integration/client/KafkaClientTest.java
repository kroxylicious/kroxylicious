/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.client;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.Response;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.codec.OpaqueRequestFrame;
import io.kroxylicious.testing.integration.server.MockServer;
import io.kroxylicious.testing.kafka.common.KeystoreManager;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaClientTest {

    private record KeystoreTrustStorePair(String brokerKeyStore, KeyStore keyStoreInstance, String clientTrustStore, String password) {}

    private static KeystoreTrustStorePair buildKeystoreTrustStorePair() throws Exception {
        var keystoreManager = new KeystoreManager();
        String dn = keystoreManager.buildDistinguishedName("test@kroxylicious.io", "localhost", "KI", "kroxylicious.io", null, null, "US");
        var bundle = keystoreManager.createSelfSignedCertificate(keystoreManager.newCertificateBuilder(dn));
        Path keystorePath = keystoreManager.generateCertificateFile(bundle);
        String password = keystoreManager.getPassword(keystorePath);
        // The generated JKS contains both the private key entry and the CA cert,
        // so the same file serves as both the proxy keystore and the client truststore.
        String keystore = keystorePath.toAbsolutePath().toString();
        KeyStore keyStoreInstance = bundle.toKeyStore(password.toCharArray());

        return new KeystoreTrustStorePair(keystore, keyStoreInstance, keystore, password);
    }

    private KeystoreTrustStorePair downstreamKeystoreTrustStorePair;

    @BeforeEach
    void beforeEach() throws Exception {
        this.downstreamKeystoreTrustStorePair = buildKeystoreTrustStorePair();
    }

    @Test
    void testClientCanSendOpaqueFrame() {
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        try (var mockServer = MockServer.startOnRandomPort(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message));
                var kafkaClient = new KafkaClient("localhost", mockServer.port())) {
            ApiVersionsRequest request = new ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0);
            int correlationId = 5;
            ByteBuffer byteBuffer = request.serializeWithHeader(new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "client", correlationId));
            int length = byteBuffer.remaining();
            OpaqueRequestFrame frame = new OpaqueRequestFrame(Unpooled.copiedBuffer(byteBuffer), correlationId, length, true, ApiKeys.API_VERSIONS, (short) 0);
            CompletableFuture<Response> future = kafkaClient.get(frame);
            assertThat(future).succeedsWithin(10, TimeUnit.SECONDS).satisfies(response -> {
                assertThat(response.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, apiVersionsRequestData -> {
                    assertThat(apiVersionsRequestData.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                });
            });
        }
    }

    // brokers can respond with a v0 response if they do not support the ApiVersions request version, see KIP-511
    @Test
    void testClientCanHandleResponseApiVersionDifferentFromRequestApiVersion() {
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        ResponsePayload v0Payload = new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message);
        try (var mockServer = MockServer.startOnRandomPort(v0Payload);
                var kafkaClient = new KafkaClient("localhost", mockServer.port())) {
            CompletableFuture<Response> future = kafkaClient.get(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData(), (short) 0));
            assertThat(future).succeedsWithin(10, TimeUnit.SECONDS).satisfies(response -> {
                assertThat(response.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, apiVersionsRequestData -> {
                    assertThat(apiVersionsRequestData.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                });
            });
        }
    }

    @Test
    void unreadBytesAfterFrameDecodeThrowsException() {
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        message.setThrottleTimeMs(22);
        ResponsePayload v0Payload = new ResponsePayload(ApiKeys.API_VERSIONS, (short) 1, message);
        try (var mockServer = MockServer.startOnRandomPort(v0Payload);
                var kafkaClient = new KafkaClient("localhost", mockServer.port())) {
            CompletableFuture<Response> future = kafkaClient.get(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData(), (short) 0));
            // fails to decode v0 response because client encodes v1, which is backwards compatible but emits additional bytes. We want to be as sure as we can be that it was a v0 response.
            assertThat(future).failsWithin(10, TimeUnit.SECONDS).withThrowableThat()
                    .withMessageContaining("Unread bytes remaining in frame, potentially response api version differs from expectation");
        }
    }

    @Test
    void unexpectedResponseFormatTriggersFailure() {
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        ResponsePayload v0Payload = new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message);
        try (var mockServer = MockServer.startOnRandomPort(v0Payload);
                var kafkaClient = new KafkaClient("localhost", mockServer.port())) {
            CompletableFuture<Response> future = kafkaClient.get(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
            // fails to decode v0 response because client expects v3
            assertThat(future).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().withMessageContaining("non-nullable field apiKeys was serialized as null");
        }
    }

    @Test
    void shouldWorkWithTls() throws Exception {
        shouldWorkWithTls(SslContextBuilder.forClient()
                .trustManager(trustManagerFactory(downstreamKeystoreTrustStorePair))
                .build());
    }

    @Test
    void shouldWorkWithTlsWhenTrustingAll() throws Exception {
        shouldWorkWithTls(KafkaClient.TRUST_ALL_CLIENT_SSL_CONTEXT);
    }

    private void shouldWorkWithTls(SslContext clientSslContext) throws SSLException {
        var message = new ApiVersionsResponseData();

        var file = new File(downstreamKeystoreTrustStorePair.brokerKeyStore());
        var serverSslContext = SslContextBuilder
                .forServer(keyManagerFactory(file, downstreamKeystoreTrustStorePair.password())).build();

        var serverResponse = new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message);

        try (var mockServer = MockServer.startOnRandomPort(serverResponse, serverSslContext);
                var kafkaClient = new KafkaClient("localhost", mockServer.port(), clientSslContext)) {

            CompletableFuture<Response> future = kafkaClient.get(new Request(ApiKeys.API_VERSIONS, (short) 0, "client", new ApiVersionsRequestData()));
            assertThat(future)
                    .succeedsWithin(10, TimeUnit.SECONDS)
                    .satisfies(response -> assertThat(response.payload().message()).isInstanceOf(ApiVersionsResponseData.class));
        }
    }

    private KeyManagerFactory keyManagerFactory(File storeFile, String password) {
        try {
            var passwordChars = Optional.ofNullable(password).map(String::toCharArray).orElse(null);
            var keyStore = downstreamKeystoreTrustStorePair.keyStoreInstance();
            var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, passwordChars);
            return keyManagerFactory;
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException("Error building KeyManagerFactory from : " + storeFile, e);
        }
    }

    private TrustManagerFactory trustManagerFactory(KeystoreTrustStorePair keystoreTrustStorePair) {
        try {
            var keyStore = keystoreTrustStorePair.keyStoreInstance();
            var trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);
            return trustManagerFactory;
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException("Error building TrustManagerFactory", e);
        }
    }
}
