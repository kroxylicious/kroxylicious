/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import io.kroxylicious.net.TlsServer;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.config.tls.FilePassword;
import io.kroxylicious.proxy.config.tls.FilePasswordFilePath;
import io.kroxylicious.proxy.config.tls.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyStore;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.TlsTestFilter;
import io.kroxylicious.proxy.filter.TlsTestFilter.Outcome;
import io.kroxylicious.proxy.filter.TlsTestFilterFactory;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.UnknownTaggedFields.unknownTaggedFieldsToStrings;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Users can use the FilterFactoryContext to obtain an {@link javax.net.ssl.SSLContext} given some
 * re-usable Tls configuration. This test checks that the context offered to the client behaves
 * as we hope, enabling a client to connect successfully to a trusted TLS server when we supply the
 * trust material in various ways. Also, we check that we can supply key material for mutual TLS auth
 * where the server checks that it trusts the client.
 *
 * Note that we discriminate between the FilterFactoryContext passed to {@link io.kroxylicious.proxy.filter.FilterFactory#initialize(FilterFactoryContext, Object)}
 * and the one passed to {@link io.kroxylicious.proxy.filter.FilterFactory#createFilter(FilterFactoryContext, Object)} as
 * there is no guarantee they are the same context with the same behaviour.
 */
class FilterTlsIT {
    @TempDir
    Path tempDir;
    @TempDir
    Path tempDir2;

    record Keys(Path keyStore, Path trustStore, Path certfilePath, String password, String keyStoreType, String trustStoreType) {}

    record TestConfig(String name, Function<Keys, Tls> filterTlsConfig, Outcome outcome) {

        @Override
        public String toString() {
            return name;
        }
    }

    private static List<TestConfig> tlsConfigurations() {
        return List.of(
                new TestConfig("insecure", (testKeys) -> insecureTls(true), Outcome.SUCCESS),
                new TestConfig("insecure disabled", (testKeys) -> insecureTls(false), Outcome.SEND_EXCEPTION),
                new TestConfig("truststore inline password", FilterTlsIT::trustStoreInlinePasswordTls, Outcome.SUCCESS),
                new TestConfig("truststore - pem format", FilterTlsIT::trustStorePemFormat, Outcome.SUCCESS),
                new TestConfig("truststore file password", FilterTlsIT::trustStoreFilePasswordTls, Outcome.SUCCESS),
                new TestConfig("truststore file password file path - deprecated", FilterTlsIT::trustStoreFilePasswordFilePathTls, Outcome.SUCCESS),
                new TestConfig("truststore with incorrect password", FilterTlsIT::trustStoreInlineWrongPasswordTls, Outcome.FAILED_TO_CREATE_SSL_CONTEXT));
    }

    @NonNull
    private static Tls trustStoreInlinePasswordTls(Keys testKeys) {
        return new Tls(null, new TrustStore(testKeys.trustStore().toString(), new InlinePassword(testKeys.password), testKeys.trustStoreType));
    }

    @NonNull
    private static Tls trustStorePemFormat(Keys testKeys) {
        Path path = TlsUtils.convertToTempFileInPemFormat(testKeys.trustStore(), testKeys.password);
        return new Tls(null, new TrustStore(path.toAbsolutePath().toString(), new InlinePassword(testKeys.password), Tls.PEM));
    }

    @NonNull
    private static Tls trustStoreFilePasswordTls(Keys testKeys) {
        File file = TlsUtils.writePasswordToTempFile(testKeys.password);
        return new Tls(null, new TrustStore(testKeys.trustStore().toString(), new FilePassword(file.getAbsolutePath()), testKeys.trustStoreType));
    }

    @NonNull
    private static Tls trustStoreFilePasswordFilePathTls(Keys testKeys) {
        File file = TlsUtils.writePasswordToTempFile(testKeys.password);
        return new Tls(null, new TrustStore(testKeys.trustStore().toString(), new FilePasswordFilePath(file.getAbsolutePath()), testKeys.trustStoreType));
    }

    @NonNull
    private static Tls trustStoreInlineWrongPasswordTls(Keys testKeys) {
        return new Tls(null, new TrustStore(testKeys.trustStore().toString(), new InlinePassword(testKeys.password + "wrong"), testKeys.trustStoreType));
    }

    @NonNull
    private static Tls insecureTls(boolean insecure) {
        return new Tls(null, new InsecureTls(insecure));
    }

    @ParameterizedTest
    @MethodSource("tlsConfigurations")
    void shouldConnectToTlsWithInsecureTlsConfigured(TestConfig config) {
        Keys keys = generateKeys(tempDir);
        TlsServer tlsServer = new TlsServer(buildSslContext(keys));
        int port = tlsServer.start();
        String endpoint = "https://localhost:" + port;

        Tls tls = config.filterTlsConfig().apply(keys);
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> proxy(bootstrap).addToFilters(tlsTestFilter(endpoint, tls)));
                var client = tester.simpleTestClient()) {
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));
            client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
            Request onlyRequest = tester.getOnlyRequest();
            Outcome initOutcome = getOutcome(onlyRequest, TlsTestFilter.INIT_SEND_SUCCESS_TAG);
            Outcome instanceOutcome = getOutcome(onlyRequest, TlsTestFilter.INSTANCE_SEND_SUCCESS_TAG);
            assertThat(initOutcome).isEqualTo(config.outcome);
            assertThat(instanceOutcome).isEqualTo(config.outcome);
        }
        finally {
            tlsServer.stop();
        }
    }

    @Test
    void testMutualTls() {
        Keys clientKeys = generateKeys(tempDir2);
        Keys serverKeys = generateKeys(tempDir);
        SslContext result = mutualTlsSslContext(serverKeys, clientKeys);
        TlsServer tlsServer = new TlsServer(result);
        int port = tlsServer.start();
        String endpoint = "https://localhost:" + port;

        try {
            assertFilterTlsConnectionHasOutcome(endpoint, mutualTlsClientConfig(clientKeys, serverKeys), Outcome.SUCCESS);
            assertFilterTlsConnectionHasOutcome(endpoint, trustStoreInlinePasswordTls(serverKeys), Outcome.SEND_EXCEPTION);
        }
        finally {
            tlsServer.stop();
        }
    }

    @NonNull
    private static Tls mutualTlsClientConfig(Keys clientKeys, Keys serverKeys) {
        return new Tls(
                new KeyStore(clientKeys.keyStore.toString(), new InlinePassword(clientKeys.password), new InlinePassword(clientKeys.password), clientKeys.keyStoreType),
                new TrustStore(serverKeys.trustStore().toString(), new InlinePassword(serverKeys.password), serverKeys.trustStoreType));
    }

    @NonNull
    private static SslContext mutualTlsSslContext(Keys serverKeys, Keys clientKeys) {
        try {
            KeyManagerFactory keyManagerFactory = keyManagerFactory(serverKeys.keyStore.toFile(), serverKeys.keyStoreType, serverKeys.password);
            return SslContextBuilder.forServer(keyManagerFactory).clientAuth(ClientAuth.REQUIRE)
                    .trustManager(trustManagerFactory(clientKeys.trustStore.toFile(), clientKeys.trustStoreType, clientKeys.password)).sslProvider(
                            SslProvider.JDK)
                    .build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertFilterTlsConnectionHasOutcome(String endpoint, Tls tls, Outcome outcome) {
        try (var tester = mockKafkaKroxyliciousTester(bootstrap -> proxy(bootstrap).addToFilters(tlsTestFilter(endpoint, tls)));
                var client = tester.simpleTestClient()) {
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.API_VERSIONS, (short) 3, new ApiVersionsResponseData()));
            client.getSync(new Request(ApiKeys.API_VERSIONS, (short) 3, "client", new ApiVersionsRequestData()));
            Request onlyRequest = tester.getOnlyRequest();
            Outcome initOutcome = getOutcome(onlyRequest, TlsTestFilter.INIT_SEND_SUCCESS_TAG);
            Outcome instanceOutcome = getOutcome(onlyRequest, TlsTestFilter.INSTANCE_SEND_SUCCESS_TAG);
            assertThat(initOutcome).isEqualTo(outcome);
            assertThat(instanceOutcome).isEqualTo(outcome);
        }
    }

    @NonNull
    private static SslContext buildSslContext(Keys keys) {
        try {
            KeyManagerFactory keyManagerFactory = keyManagerFactory(keys.keyStore.toFile(), keys.keyStoreType, keys.password);
            return SslContextBuilder.forServer(keyManagerFactory).build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Keys generateKeys(Path tempDir) {
        try {
            Path keystorePath = tempDir.resolve("keystore.p12");
            Path truststorePath = tempDir.resolve("truststore.p12");
            KeytoolCertificateGenerator gen = new KeytoolCertificateGenerator(keystorePath.toString(), truststorePath.toString());
            gen.generateSelfSignedCertificateEntry("fake@email.com", "localhost", "org", "org", "org", "CAN", "NZ");
            gen.generateTrustStore(gen.getCertFilePath(), "test", gen.getTrustStoreLocation());
            return new Keys(keystorePath, truststorePath, Path.of(gen.getCertFilePath()), gen.getPassword(), gen.getKeyStoreType(), gen.getTrustStoreType());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NonNull
    private static Outcome getOutcome(Request onlyRequest, int tag) {
        return Outcome.valueOf(unknownTaggedFieldsToStrings(onlyRequest.message(), tag).findFirst().orElseThrow());
    }

    private static FilterDefinition tlsTestFilter(String endpoint, Tls tls) {
        return new FilterDefinitionBuilder(TlsTestFilterFactory.class.getName()).withConfig(Map.of("endpoint", endpoint, "tls", tls))
                .build();
    }

    private static KeyManagerFactory keyManagerFactory(File keyStoreFile, String type, String password) {
        try (var is = new FileInputStream(keyStoreFile)) {
            var keyStore = java.security.KeyStore.getInstance(type);
            keyStore.load(is, password.toCharArray());
            var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, password.toCharArray());
            return keyManagerFactory;
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Error building KeyManagerFactory from : " + keyStoreFile, e);
        }
    }

    private static TrustManagerFactory trustManagerFactory(File trustManagerFile, String type, String password) {
        try (var is = new FileInputStream(trustManagerFile)) {
            var trustStore = java.security.KeyStore.getInstance(type);
            trustStore.load(is, password.toCharArray());
            var trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            return trustManagerFactory;
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Error building TrustManagerFactory from : " + trustManagerFile, e);
        }
    }
}
