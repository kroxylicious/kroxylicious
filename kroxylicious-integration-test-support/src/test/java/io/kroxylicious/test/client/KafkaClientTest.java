/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
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
import org.junit.jupiter.api.io.TempDir;

import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.codec.OpaqueRequestFrame;
import io.kroxylicious.test.server.MockServer;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaClientTest {

    private KeytoolCertificateGenerator downstreamCertificateGenerator;
    private Path clientTrustStore;

    @TempDir
    private Path certsDirectory;

    @BeforeEach
    void beforeEach() throws Exception {
        // Note that the KeytoolCertificateGenerator generates key stores that are PKCS12 format.
        this.downstreamCertificateGenerator = new KeytoolCertificateGenerator();
        this.downstreamCertificateGenerator.generateSelfSignedCertificateEntry("test@kroxylicious.io", "localhost", "KI", "kroxylicious.io", null, null, "US");
        this.clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        this.downstreamCertificateGenerator.generateTrustStore(this.downstreamCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());
    }

    @Test
    void testClientCanSendOpaqueFrame() {
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        try (var mockServer = MockServer.startOnRandomPort(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message));
                var kafkaClient = new KafkaClient("127.0.0.1", mockServer.port())) {
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
    void testClientCanTolerateV0ApiVersionsResponseToHigherRequestVersion() {
        ApiVersionsResponseData message = new ApiVersionsResponseData();
        message.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        ResponsePayload v0Payload = new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message);
        try (var mockServer = MockServer.startOnRandomPort(v0Payload);
                var kafkaClient = new KafkaClient("127.0.0.1", mockServer.port())) {
            CompletableFuture<Response> future = kafkaClient.get(new Request(ApiKeys.API_VERSIONS, (short) 0, "client", new ApiVersionsRequestData()));
            assertThat(future).succeedsWithin(10, TimeUnit.SECONDS).satisfies(response -> {
                assertThat(response.payload().message()).isInstanceOfSatisfying(ApiVersionsResponseData.class, apiVersionsRequestData -> {
                    assertThat(apiVersionsRequestData.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
                });
            });
        }
    }

    @Test
    void shouldWorkWithTls() throws Exception {
        shouldWorkWithTls(SslContextBuilder.forClient()
                .trustManager(
                        trustManagerFactory(clientTrustStore.toFile(), downstreamCertificateGenerator.getKeyStoreType(), downstreamCertificateGenerator.getPassword()))
                .build());
    }

    @Test
    void shouldWorkWithTlsWhenTrustingAll() throws Exception {
        shouldWorkWithTls(KafkaClient.TRUST_ALL_CLIENT_SSL_CONTEXT);
    }

    private void shouldWorkWithTls(SslContext clientSslContext) throws SSLException {
        var message = new ApiVersionsResponseData();

        var file = new File(downstreamCertificateGenerator.getKeyStoreLocation());
        var serverSslContext = SslContextBuilder
                .forServer(keyManagerFactory(file, downstreamCertificateGenerator.getKeyStoreType(), downstreamCertificateGenerator.getPassword())).build();

        var serverResponse = new ResponsePayload(ApiKeys.API_VERSIONS, (short) 0, message);

        try (var mockServer = MockServer.startOnRandomPort(serverResponse, serverSslContext);
                var kafkaClient = new KafkaClient("127.0.0.1", mockServer.port(), clientSslContext)) {

            CompletableFuture<Response> future = kafkaClient.get(new Request(ApiKeys.API_VERSIONS, (short) 0, "client", new ApiVersionsRequestData()));
            assertThat(future)
                    .succeedsWithin(10, TimeUnit.SECONDS)
                    .satisfies(response -> assertThat(response.payload().message()).isInstanceOf(ApiVersionsResponseData.class));
        }
    }

    private KeyManagerFactory keyManagerFactory(File storeFile, String keyStoreType, String password) {
        try {
            var passwordChars = Optional.ofNullable(password).map(String::toCharArray).orElse(null);
            var keyStore = loadKeyStore(storeFile, keyStoreType, passwordChars);
            var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, passwordChars);
            return keyManagerFactory;
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException("Error building KeyManagerFactory from : " + storeFile, e);
        }
    }

    private TrustManagerFactory trustManagerFactory(File storeFile, String keyStoreType, String password) {

        try {
            var passwordChars = Optional.ofNullable(password).map(String::toCharArray).orElse(null);
            var keyStore = loadKeyStore(storeFile, keyStoreType, passwordChars);
            var trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);
            return trustManagerFactory;
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException("Error building TrustManagerFactory from : " + storeFile, e);
        }

    }

    private KeyStore loadKeyStore(File storeFile, String keyStoreType, char[] passwordChars) {
        try (var is = new FileInputStream(storeFile)) {
            var keyStore = KeyStore.getInstance(keyStoreType);
            keyStore.load(is, passwordChars);
            return keyStore;
        }
        catch (CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
