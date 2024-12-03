/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.FileNotFoundException;
import java.security.UnrecoverableKeyException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class NettyTrustProviderTest {
    private final SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

    public static Stream<Arguments> trustStoreTypes() {
        return Stream.of(
                Arguments.of("Platform Default Store JKS", null, "client.jks", TlsTestConstants.STOREPASS),
                Arguments.of("JKS", TlsTestConstants.JKS, "client.jks", TlsTestConstants.STOREPASS),
                Arguments.of("PKCS12", TlsTestConstants.PKCS_12, "server.p12", TlsTestConstants.STOREPASS),
                Arguments.of("Certificate PEM passed as keyStore (KIP-651)", TlsTestConstants.PEM, "server.crt", TlsTestConstants.STOREPASS),
                Arguments.of("JKS store password from file", TlsTestConstants.JKS, "client.jks", TlsTestConstants.KEYSTORE_FILE_PASSWORD));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource()
    void trustStoreTypes(String name, String storeType, String storeFile, PasswordProvider storePassword) throws Exception {
        var trustStore = new NettyTrustProvider(new TrustStore(TlsTestConstants.getResourceLocationOnFilesystem(storeFile), storePassword, storeType, null));
        trustStore.apply(sslContextBuilder);
        var sslContext = sslContextBuilder.build();
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isClient()).isTrue();
        assertThat(sslContextBuilder).extracting("endpointIdentificationAlgorithm").isEqualTo("HTTPS");
    }

    @Test
    void trustStoreIncorrectPassword() {
        var trustStore = new NettyTrustProvider(new TrustStore(TlsTestConstants.getResourceLocationOnFilesystem("client.jks"), TlsTestConstants.BADPASS, null, null));
        assertThatCode(() -> trustStore.apply(sslContextBuilder))
                .hasMessageContaining("Error building SSLContext")
                .hasRootCauseInstanceOf(UnrecoverableKeyException.class);
    }

    @ParameterizedTest
    @EnumSource(TlsClientAuth.class)
    void clientAuthentication(TlsClientAuth clientAuth) {
        var trustStore = new NettyTrustProvider(
                new TrustStore(TlsTestConstants.getResourceLocationOnFilesystem("client.jks"), TlsTestConstants.STOREPASS, TlsTestConstants.JKS, clientAuth));
        trustStore.apply(sslContextBuilder);
        assertThat(sslContextBuilder)
                .extracting("clientAuth")
                .satisfies(nettyClientAuth -> {
                    switch (clientAuth) {
                        case REQUIRED -> assertThat(nettyClientAuth).isEqualTo(ClientAuth.REQUIRE);
                        case REQUESTED -> assertThat(nettyClientAuth).isEqualTo(ClientAuth.OPTIONAL);
                        case NONE -> assertThat(nettyClientAuth).isEqualTo(ClientAuth.NONE);
                    }

                });
    }

    @Test
    void supportsClientAuthenticationDisabled() {
        var trustStore = new NettyTrustProvider(
                new TrustStore(TlsTestConstants.getResourceLocationOnFilesystem("client.jks"), TlsTestConstants.STOREPASS, TlsTestConstants.JKS, null));
        trustStore.apply(sslContextBuilder);
        assertThat(sslContextBuilder)
                .extracting("clientAuth")
                .satisfies(nettyClientAuth -> {
                    assertThat(nettyClientAuth).isEqualTo(ClientAuth.NONE);
                });
    }

    @Test
    void trustStoreNotFound() {
        var trustStore = new NettyTrustProvider(new TrustStore(TlsTestConstants.NOT_EXIST, TlsTestConstants.STOREPASS, null, null));
        assertThatCode(() -> trustStore.apply(sslContextBuilder))
                .hasMessageContaining("Error building SSLContext")
                .hasRootCauseInstanceOf(FileNotFoundException.class);
    }

    @Test
    void shouldDisableHostnameVerification() {
        // Given
        var trustStore = new NettyTrustProvider(new InsecureTls(true));

        // When
        trustStore.apply(sslContextBuilder);

        // Then
        assertThat(sslContextBuilder).extracting("endpointIdentificationAlgorithm").isNull();
    }

    @Test
    void shouldEnableHostnameVerification() {
        // Given
        var trustStore = new NettyTrustProvider(new InsecureTls(false));

        // When
        trustStore.apply(sslContextBuilder);

        // Then
        assertThat(sslContextBuilder).extracting("endpointIdentificationAlgorithm").isEqualTo("HTTPS");
    }

    @Test
    void shouldEnableHostnameVerificationForPlatformTrust() {
        // Given
        var trustStore = new NettyTrustProvider(PlatformTrustProvider.INSTANCE);

        // When
        trustStore.apply(sslContextBuilder);

        // Then
        assertThat(sslContextBuilder).extracting("endpointIdentificationAlgorithm").isEqualTo("HTTPS");
    }
}
