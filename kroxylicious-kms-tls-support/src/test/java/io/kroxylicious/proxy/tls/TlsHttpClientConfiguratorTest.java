/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.KeyStore;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;

import static io.kroxylicious.proxy.tls.CertificateGenerator.createJksKeystore;
import static io.kroxylicious.proxy.tls.CertificateGenerator.generateRsaKeyPair;
import static io.kroxylicious.proxy.tls.CertificateGenerator.generateSelfSignedX509Certificate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class TlsHttpClientConfiguratorTest {
    private static final X509Certificate SELF_SIGNED_X_509_CERTIFICATE = generateSelfSignedX509Certificate(generateRsaKeyPair());

    private static final String KNOWN_CIPHER_SUITE1;
    private static final String KNOWN_CIPHER_SUITE2;

    private static final String TLS_V1_3 = "TLSv1.3";
    private static final String TLS_V1_2 = "TLSv1.2";
    private static final String TLS_V1_1_KNOWN_BUT_NOT_DEFAULT = "TLSv1.1";

    static {
        try {
            var defaultSslContext = SSLContext.getDefault();
            var defaultSSLParameters = defaultSslContext.getDefaultSSLParameters();
            var supportedSSLParameters = defaultSslContext.getSupportedSSLParameters();
            KNOWN_CIPHER_SUITE1 = defaultSSLParameters.getCipherSuites()[0];
            KNOWN_CIPHER_SUITE2 = defaultSSLParameters.getCipherSuites()[1];
            assertThat(KNOWN_CIPHER_SUITE1).isNotNull();
            assertThat(KNOWN_CIPHER_SUITE2).isNotNull().isNotEqualTo(KNOWN_CIPHER_SUITE1);
            assertThat(defaultSSLParameters.getProtocols())
                    .contains(TLS_V1_2, TLS_V1_3)
                    .doesNotContain(TLS_V1_1_KNOWN_BUT_NOT_DEFAULT);
            assertThat(supportedSSLParameters.getProtocols())
                    .contains(TLS_V1_2, TLS_V1_3, TLS_V1_1_KNOWN_BUT_NOT_DEFAULT);
        }
        catch (NoSuchAlgorithmException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final HttpClient.Builder builder = HttpClient.newBuilder();

    @Test
    void testInsecureTlsEnabled() {
        InsecureTls insecureTls = new InsecureTls(true);
        TrustManager[] trustManagers = TlsHttpClientConfigurator.getTrustManagers(insecureTls);
        for (TrustManager trustManager : trustManagers) {
            assertThat(trustManager).isInstanceOfSatisfying(X509TrustManager.class, x509TrustManager -> {
                assertThat(x509TrustManager.getAcceptedIssuers()).isNotNull().isEmpty();
                assertThatCode(() -> x509TrustManager.checkClientTrusted(new X509Certificate[]{ SELF_SIGNED_X_509_CERTIFICATE }, "any")).doesNotThrowAnyException();
                assertThatCode(() -> x509TrustManager.checkServerTrusted(new X509Certificate[]{ SELF_SIGNED_X_509_CERTIFICATE }, "any")).doesNotThrowAnyException();
            });
        }
    }

    @Test
    void testInsecureTlsDisabled() {
        InsecureTls insecureTlsDisabled = new InsecureTls(false);
        TrustManager[] trustManagers = TlsHttpClientConfigurator.getTrustManagers(insecureTlsDisabled);
        for (TrustManager trustManager : trustManagers) {
            assertThat(trustManager).isInstanceOfSatisfying(X509TrustManager.class, x509TrustManager -> {
                assertThat(x509TrustManager.getAcceptedIssuers()).isNotNull().isNotEmpty();
                assertThatThrownBy(() -> x509TrustManager.checkClientTrusted(new X509Certificate[]{ SELF_SIGNED_X_509_CERTIFICATE }, "any")).isInstanceOf(
                        CertificateException.class);
                assertThatThrownBy(() -> x509TrustManager.checkServerTrusted(new X509Certificate[]{ SELF_SIGNED_X_509_CERTIFICATE }, "any")).isInstanceOf(
                        CertificateException.class);
            });
        }
    }

    @Test
    void testNullTrustManagersResultsInDefaultSslContext() throws NoSuchAlgorithmException {
        var tls = new TlsHttpClientConfigurator(new Tls(null, null, null, null));

        tls.apply(builder);

        assertThat(builder.build().sslContext()).isSameAs(SSLContext.getDefault());
    }

    @Test
    void testSslContextProtocolIsTlsIfWeSupplyTrust() {
        var tls = new TlsHttpClientConfigurator(new Tls(null, new InsecureTls(true), null, null));

        tls.apply(builder);

        assertThat(builder.build().sslContext().getProtocol()).isEqualTo("TLS");
    }

    @Test
    void testNullTlsResultsInDefaultSslContext() throws NoSuchAlgorithmException {
        var tls = new TlsHttpClientConfigurator(null);

        tls.apply(builder);

        assertThat(builder.build().sslContext()).isSameAs(SSLContext.getDefault());
    }

    @Test
    void testFileNotFound() {
        TrustStore store = new TrustStore("/tmp/" + UUID.randomUUID(), new InlinePassword("changeit"), null, null);
        assertThatThrownBy(() -> TlsHttpClientConfigurator.getTrustManagers(store)).isInstanceOf(SslConfigurationException.class).cause()
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testJks() {
        CertificateGenerator.Keys keys = CertificateGenerator.generate();
        CertificateGenerator.TrustStore trustStore = keys.jksClientTruststore();
        TrustStore store = new TrustStore(trustStore.path().toString(), new InlinePassword(trustStore.password()), null, null);
        TrustManager[] trustManagers = TlsHttpClientConfigurator.getTrustManagers(store);
        assertThat(trustManagers).isNotEmpty();
    }

    @Test
    void testPemNotSupported() {
        TrustStore store = new TrustStore("/tmp/store", null, "PEM", null);
        assertThatThrownBy(() -> {
            TlsHttpClientConfigurator.getTrustManagers(store);
        }).isInstanceOf(SslConfigurationException.class).hasMessage("PEM trust not supported by vault yet");
    }

    @Test
    void testJksWrongPassword() {
        CertificateGenerator.Keys keys = CertificateGenerator.generate();
        CertificateGenerator.TrustStore trustStore = keys.jksClientTruststore();
        String badPassword = UUID.randomUUID().toString();
        TrustStore store = new TrustStore(trustStore.path().toString(), new InlinePassword(badPassword), null, null);
        assertThatThrownBy(() -> TlsHttpClientConfigurator.getTrustManagers(store)).isInstanceOf(SslConfigurationException.class).cause().isInstanceOf(IOException.class)
                .hasMessageContaining("Keystore was tampered with, or password was incorrect");
    }

    @Test
    void testPkcs12() {
        CertificateGenerator.Keys keys = CertificateGenerator.generate();
        CertificateGenerator.TrustStore trustStore = keys.pkcs12ClientTruststore();
        TrustStore store = new TrustStore(trustStore.path().toString(), new InlinePassword(trustStore.password()), trustStore.type(), null);
        TrustManager[] trustManagers = TlsHttpClientConfigurator.getTrustManagers(store);
        assertThat(trustManagers).isNotEmpty();
    }

    @Test
    void testKeyStore() {
        CertificateGenerator.Keys keys = CertificateGenerator.generate();
        CertificateGenerator.KeyStore keyStore = keys.jksServerKeystore();
        KeyStore store = new KeyStore(keyStore.path().toString(), new InlinePassword(keyStore.storePassword()), new InlinePassword(keyStore.keyPassword()),
                keyStore.type());
        KeyManager[] trustManagers = TlsHttpClientConfigurator.getKeyManagers(store);
        assertThat(trustManagers).isNotEmpty();
    }

    @Test
    void testKeyStoreKeyPasswordDefaultsToStorePassword() {
        java.security.KeyPair pair = generateRsaKeyPair();
        X509Certificate x509Certificate = generateSelfSignedX509Certificate(pair);
        String password = "password";
        CertificateGenerator.KeyStore keyStore = createJksKeystore(pair, x509Certificate, password, password);
        KeyStore store = new KeyStore(keyStore.path().toString(), new InlinePassword(keyStore.storePassword()), null,
                keyStore.type());
        KeyManager[] trustManagers = TlsHttpClientConfigurator.getKeyManagers(store);
        assertThat(trustManagers).isNotEmpty();
    }

    @Test
    void testKeyPairNotSupported() {
        KeyPair store = new KeyPair("/tmp/keypair", "/tmp/cert", null);
        assertThatThrownBy(() -> {
            TlsHttpClientConfigurator.getKeyManagers(store);
        }).isInstanceOf(SslConfigurationException.class).hasMessageContaining("KeyPair is not supported by this client");
    }

    @Test
    void testKeyStorePemNotSupported() {
        KeyStore store = new KeyStore("/tmp/pem", null, null, "PEM");
        assertThatThrownBy(() -> {
            TlsHttpClientConfigurator.getKeyManagers(store);
        }).isInstanceOf(SslConfigurationException.class).hasMessageContaining("PEM is not supported by this client");
    }

    @Test
    void testPkcs12WrongPassword() {
        CertificateGenerator.Keys keys = CertificateGenerator.generate();
        CertificateGenerator.TrustStore trustStore = keys.pkcs12ClientTruststore();
        String badPassword = UUID.randomUUID().toString();
        TrustStore store = new TrustStore(trustStore.path().toString(), new InlinePassword(badPassword), null, null);
        assertThatThrownBy(() -> TlsHttpClientConfigurator.getTrustManagers(store)).isInstanceOf(SslConfigurationException.class).cause().isInstanceOf(IOException.class)
                .hasMessageContaining("keystore password was incorrect");
    }

    static Stream<Arguments> tlsProtocolRestrictions() {
        return Stream.of(
                argumentSet("allow one",
                        new AllowDeny<>(List.of(TLS_V1_3), null),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getProtocols()).containsExactly(TLS_V1_3)),
                argumentSet("deny only",
                        new AllowDeny<>(null, Set.of(TLS_V1_3)),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getProtocols()).doesNotContain(TLS_V1_3)),
                argumentSet("deny filters allow",
                        new AllowDeny<>(List.of(TLS_V1_3, TLS_V1_2, TLS_V1_1_KNOWN_BUT_NOT_DEFAULT), Set.of(TLS_V1_2)),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getProtocols()).containsExactly(TLS_V1_3, TLS_V1_1_KNOWN_BUT_NOT_DEFAULT)),
                argumentSet("permits enablement of supported protocol",
                        new AllowDeny<>(List.of(TLS_V1_1_KNOWN_BUT_NOT_DEFAULT), null),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getProtocols()).containsExactly(TLS_V1_1_KNOWN_BUT_NOT_DEFAULT)),
                argumentSet("ignores unknown protocols",
                        new AllowDeny<>(List.of(TLS_V1_3, TLS_V1_2, "UNKNOWN_PROTOCOL1"), Set.of(TLS_V1_2, "UNKNOWN_PROTOCOL2")),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getProtocols()).containsExactly(TLS_V1_3)));
    }

    @ParameterizedTest
    @MethodSource(value = "tlsProtocolRestrictions")
    @SuppressWarnings("java:S6103") // false positive - consumer argument is asserting something
    void restrictTlsProtocol(AllowDeny<String> allowedDenied, Consumer<SSLParameters> assertSatisfies) {
        var tls = new TlsHttpClientConfigurator(new Tls(null, null, null, allowedDenied));
        tls.apply(builder);

        assertThat(builder.build().sslParameters()).satisfies(assertSatisfies::accept);
    }

    @Test
    void detectsNoEnabledProtocols() {
        var tls = new TlsHttpClientConfigurator(new Tls(null, null, null, new AllowDeny<>(List.of(TLS_V1_3), Set.of(TLS_V1_3))));
        assertThatThrownBy(() -> tls.apply(builder))
                .isInstanceOf(SslConfigurationException.class);
    }

    static Stream<Arguments> tlsCipherSuiteRestrictions() {
        return Stream.of(
                argumentSet("allow one",
                        new AllowDeny<>(List.of(KNOWN_CIPHER_SUITE1), null),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getCipherSuites()).containsExactly(KNOWN_CIPHER_SUITE1)),
                argumentSet("deny only",
                        new AllowDeny<>(null, Set.of(KNOWN_CIPHER_SUITE1)),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getCipherSuites()).doesNotContain(KNOWN_CIPHER_SUITE1)),
                argumentSet("deny filters allow",
                        new AllowDeny<>(List.of(KNOWN_CIPHER_SUITE1, KNOWN_CIPHER_SUITE2), Set.of(KNOWN_CIPHER_SUITE1)),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getCipherSuites()).containsExactly(KNOWN_CIPHER_SUITE2)),
                argumentSet("ignores unknown cipher suites",
                        new AllowDeny<>(List.of(KNOWN_CIPHER_SUITE1, KNOWN_CIPHER_SUITE2, "UNKNOWN_CIPHER_SUITE1"), Set.of(KNOWN_CIPHER_SUITE2, "UNKNOWN_CIPHER_SUITE2")),
                        (Consumer<SSLParameters>) sslParams -> assertThat(sslParams.getCipherSuites()).containsExactly(KNOWN_CIPHER_SUITE1)));
    }

    @ParameterizedTest
    @MethodSource(value = "tlsCipherSuiteRestrictions")
    @SuppressWarnings("java:S6103") // false positive - consumer argument is asserting something
    void restrictTlsCipherSuites(AllowDeny<String> allowedDenied, Consumer<SSLParameters> assertSatisfies) {
        var tls = new TlsHttpClientConfigurator(new Tls(null, null, allowedDenied, null));
        tls.apply(builder);

        assertThat(builder.build().sslParameters()).satisfies(assertSatisfies::accept);
    }

    @Test
    void detectsNoEnabledCipherSuites() {
        var tls = new TlsHttpClientConfigurator(new Tls(null, null, new AllowDeny<>(List.of(KNOWN_CIPHER_SUITE1), Set.of(KNOWN_CIPHER_SUITE1)), null));
        assertThatThrownBy(() -> tls.apply(builder))
                .isInstanceOf(SslConfigurationException.class);
    }
}
