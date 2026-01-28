/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to verify backward compatibility of TLS configuration.
 * Ensures that existing configurations without tlsCredentialSupplier continue to work.
 */
class TlsBackwardCompatibilityTest {

    @Test
    void testTlsConfigWithoutCredentialSupplierIsValid() {
        // Given - TLS config without credential supplier (pre-plugin feature)
        Tls tls = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                new TrustStore("/path/to/truststore", null, "JKS"),
                null,
                null,
                null // No credential supplier
        );

        // Then
        assertThat(tls.definesKey()).isTrue();
        assertThat(tls.key()).isNotNull();
        assertThat(tls.trust()).isNotNull();
        assertThat(tls.tlsCredentialSupplier()).isNull();
    }

    @Test
    void testTlsConfigWithCredentialSupplierIsValid() {
        // Given - TLS config with credential supplier (new feature)
        TlsCredentialSupplierDefinition supplierDef = new TlsCredentialSupplierDefinition("MySupplier", null);

        Tls tls = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                new TrustStore("/path/to/truststore", null, "JKS"),
                null,
                null,
                supplierDef);

        // Then
        assertThat(tls.definesKey()).isTrue();
        assertThat(tls.key()).isNotNull();
        assertThat(tls.trust()).isNotNull();
        assertThat(tls.tlsCredentialSupplier()).isNotNull();
        assertThat(tls.tlsCredentialSupplier().type()).isEqualTo("MySupplier");
    }

    @Test
    void testTlsConfigWithOnlyCredentialSupplierIsValid() {
        // Given - TLS config with only credential supplier (no static key/trust)
        TlsCredentialSupplierDefinition supplierDef = new TlsCredentialSupplierDefinition("DynamicSupplier",
                new Object());

        Tls tls = new Tls(null, null, null, null, supplierDef);

        // Then - credential supplier can work without static key
        assertThat(tls.definesKey()).isFalse();
        assertThat(tls.key()).isNull();
        assertThat(tls.trust()).isNull();
        assertThat(tls.tlsCredentialSupplier()).isNotNull();
    }

    @Test
    void testTlsConfigWithBothStaticAndDynamicCredentials() {
        // Given - TLS config with both static credentials and credential supplier
        TlsCredentialSupplierDefinition supplierDef = new TlsCredentialSupplierDefinition("FallbackSupplier", null);

        Tls tls = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                null,
                null,
                null,
                supplierDef);

        // Then - both are available
        assertThat(tls.definesKey()).isTrue();
        assertThat(tls.key()).isNotNull();
        assertThat(tls.tlsCredentialSupplier()).isNotNull();
    }

    @Test
    void testMinimalTlsConfigWithoutCredentialSupplier() {
        // Given - minimal TLS config (backward compatible)
        Tls tls = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                null, // platform trust
                null, // default cipher suites
                null, // default protocols
                null // no credential supplier
        );

        // Then
        assertThat(tls.definesKey()).isTrue();
        assertThat(tls.key()).isNotNull();
        assertThat(tls.trust()).isNull();
        assertThat(tls.cipherSuites()).isNull();
        assertThat(tls.protocols()).isNull();
        assertThat(tls.tlsCredentialSupplier()).isNull();
    }

    @Test
    void testEmptyTlsConfigIsValid() {
        // Given - empty TLS config (all fields null)
        Tls tls = new Tls(null, null, null, null);

        // Then
        assertThat(tls.definesKey()).isFalse();
        assertThat(tls.key()).isNull();
        assertThat(tls.trust()).isNull();
        assertThat(tls.cipherSuites()).isNull();
        assertThat(tls.protocols()).isNull();
        assertThat(tls.tlsCredentialSupplier()).isNull();
    }

    @Test
    void testTlsConfigWithCipherSuitesAndProtocols() {
        // Given - TLS config with cipher suites and protocols (without credential supplier)
        AllowDeny<String> cipherSuites = new AllowDeny<>(
                java.util.List.of("TLS_AES_256_GCM_SHA384"),
                java.util.Set.of("TLS_RSA_WITH_NULL_SHA"));
        AllowDeny<String> protocols = new AllowDeny<>(
                java.util.List.of("TLSv1.3"),
                java.util.Set.of("TLSv1.0"));

        Tls tls = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                null,
                cipherSuites,
                protocols,
                null);

        // Then - all fields accessible
        assertThat(tls.definesKey()).isTrue();
        assertThat(tls.cipherSuites()).isNotNull();
        assertThat(tls.cipherSuites().allowed()).contains("TLS_AES_256_GCM_SHA384");
        assertThat(tls.protocols()).isNotNull();
        assertThat(tls.protocols().allowed()).contains("TLSv1.3");
        assertThat(tls.tlsCredentialSupplier()).isNull();
    }

    @Test
    void testTlsConfigWithAllFeaturesIncludingCredentialSupplier() {
        // Given - comprehensive TLS config with all features
        TlsCredentialSupplierDefinition supplierDef = new TlsCredentialSupplierDefinition("AdvancedSupplier",
                java.util.Map.of("option", "value"));

        AllowDeny<String> cipherSuites = new AllowDeny<>(
                java.util.List.of("TLS_AES_256_GCM_SHA384"),
                java.util.Set.of());
        AllowDeny<String> protocols = new AllowDeny<>(
                java.util.List.of("TLSv1.3", "TLSv1.2"),
                java.util.Set.of());

        Tls tls = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                new TrustStore("/path/to/truststore", null, "JKS"),
                cipherSuites,
                protocols,
                supplierDef);

        // Then - all features are present and accessible
        assertThat(tls.definesKey()).isTrue();
        assertThat(tls.key()).isNotNull();
        assertThat(tls.trust()).isNotNull();
        assertThat(tls.cipherSuites()).isNotNull();
        assertThat(tls.protocols()).isNotNull();
        assertThat(tls.tlsCredentialSupplier()).isNotNull();
        assertThat(tls.tlsCredentialSupplier().type()).isEqualTo("AdvancedSupplier");
        assertThat(tls.tlsCredentialSupplier().config()).isNotNull();
    }

    @Test
    void testDefinesKeyMethodWithoutCredentialSupplier() {
        // Given - configs with and without key provider
        Tls tlsWithKey = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                null, null, null);
        Tls tlsWithoutKey = new Tls(null, null, null, null);

        // Then
        assertThat(tlsWithKey.definesKey()).isTrue();
        assertThat(tlsWithoutKey.definesKey()).isFalse();
    }

    @Test
    void testDefinesKeyMethodIsIndependentOfCredentialSupplier() {
        // Given - config with credential supplier but no static key
        TlsCredentialSupplierDefinition supplierDef = new TlsCredentialSupplierDefinition("DynamicSupplier", null);
        Tls tls = new Tls(null, null, null, null, supplierDef);

        // Then - definesKey() only checks for static key provider
        assertThat(tls.definesKey()).isFalse();
        assertThat(tls.tlsCredentialSupplier()).isNotNull();
    }

    @Test
    void testCredentialSupplierConfigCanBeNull() {
        // Given - credential supplier without config (Void config type)
        TlsCredentialSupplierDefinition supplierDef = new TlsCredentialSupplierDefinition("NoConfigSupplier", null);

        // Then - config is null but type is present
        assertThat(supplierDef.type()).isEqualTo("NoConfigSupplier");
        assertThat(supplierDef.config()).isNull();
    }

    @Test
    void testCredentialSupplierConfigCanBeObject() {
        // Given - credential supplier with complex config
        record SupplierConfig(String endpoint, int timeout) {}
        SupplierConfig config = new SupplierConfig("https://kms.example.com", 30);
        TlsCredentialSupplierDefinition supplierDef = new TlsCredentialSupplierDefinition("KmsSupplier", config);

        // Then - config is preserved
        assertThat(supplierDef.type()).isEqualTo("KmsSupplier");
        assertThat(supplierDef.config()).isNotNull();
        assertThat(supplierDef.config()).isInstanceOf(SupplierConfig.class);
        assertThat(((SupplierConfig) supplierDef.config()).endpoint()).isEqualTo("https://kms.example.com");
    }

    @Test
    void testFourParameterConstructorForBackwardCompatibility() {
        // Given - using old 4-parameter constructor (v0.18.0 compatibility)
        Tls tls = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                new TrustStore("/path/to/truststore", null, "JKS"),
                null,
                null); // 4 parameters only - no tlsCredentialSupplier

        // Then - should behave identically to 5-parameter version with null
        assertThat(tls.definesKey()).isTrue();
        assertThat(tls.key()).isNotNull();
        assertThat(tls.trust()).isNotNull();
        assertThat(tls.cipherSuites()).isNull();
        assertThat(tls.protocols()).isNull();
        assertThat(tls.tlsCredentialSupplier()).isNull();
    }

    @Test
    void testFourParameterConstructorWithCipherSuitesAndProtocols() {
        // Given - 4-parameter constructor with all legacy options
        AllowDeny<String> cipherSuites = new AllowDeny<>(
                java.util.List.of("TLS_AES_256_GCM_SHA384"),
                java.util.Set.of("TLS_RSA_WITH_NULL_SHA"));
        AllowDeny<String> protocols = new AllowDeny<>(
                java.util.List.of("TLSv1.3"),
                java.util.Set.of("TLSv1.0"));

        Tls tls = new Tls(
                new KeyPair("/path/to/key", "/path/to/cert", null),
                new TrustStore("/path/to/truststore", null, "JKS"),
                cipherSuites,
                protocols); // 4 parameters only

        // Then - all legacy fields accessible, credential supplier is null
        assertThat(tls.definesKey()).isTrue();
        assertThat(tls.cipherSuites()).isNotNull();
        assertThat(tls.protocols()).isNotNull();
        assertThat(tls.tlsCredentialSupplier()).isNull();
    }
}
