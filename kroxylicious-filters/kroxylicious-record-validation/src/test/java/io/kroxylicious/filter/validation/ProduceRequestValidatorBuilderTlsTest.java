/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation;

import java.io.FileOutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.filter.validation.config.BytebufValidation;
import io.kroxylicious.filter.validation.config.SchemaValidationConfig;
import io.kroxylicious.filter.validation.config.TopicMatchingRecordValidationRule;
import io.kroxylicious.filter.validation.config.ValidationConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.PlatformTrustProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProduceRequestValidatorBuilderTlsTest {

    // This test uses a copy of the same key material as io.kroxylicious.proxy.config.tls.TlsTestConstants in kroxylicious_runtime
    private static final String PATH_TO_TRUSTSTORE_JKS = getResourceLocationOnFilesystem("client.jks");
    private static final String PATH_TO_TRUSTSTORE_P12 = getResourceLocationOnFilesystem("client.p12");
    private static final String PATH_TO_CLIENT_CERT_PEM = getResourceLocationOnFilesystem("client.pem");
    public static final String STORE_PASS = "storepass";
    private static URL registryUrl;
    private static String jksTruststorePath;
    private static String pkcs12TruststorePath;
    private static String pemCertPath;
    private static final String TRUSTSTORE_PASSWORD = "changeit";

    @TempDir
    private static Path tempDir;

    @BeforeAll
    static void init() throws Exception {
        registryUrl = URI.create("http://localhost:8080").toURL();
        createTrustStoreFiles();
    }

    private static void createTrustStoreFiles() throws Exception {
        // Generate a self-signed keystore using keytool, then extract the cert
        var jksPath = tempDir.resolve("truststore.jks");
        jksTruststorePath = jksPath.toString();

        new ProcessBuilder("keytool", "-genkeypair", "-alias", "test", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "1", "-dname", "CN=test", "-keystore", jksTruststorePath,
                "-storepass", TRUSTSTORE_PASSWORD, "-keypass", TRUSTSTORE_PASSWORD, "-storetype", "JKS")
                .inheritIO().start().waitFor();

        // Load the certificate from JKS
        var jksStore = KeyStore.getInstance("JKS");
        try (var is = Files.newInputStream(jksPath)) {
            jksStore.load(is, TRUSTSTORE_PASSWORD.toCharArray());
        }
        Certificate cert = jksStore.getCertificate("test");

        // Create PKCS12 truststore with just the cert (no key)
        var p12Store = KeyStore.getInstance("PKCS12");
        p12Store.load(null, TRUSTSTORE_PASSWORD.toCharArray());
        p12Store.setCertificateEntry("test", cert);
        pkcs12TruststorePath = tempDir.resolve("truststore.p12").toString();
        try (var fos = new FileOutputStream(pkcs12TruststorePath)) {
            p12Store.store(fos, TRUSTSTORE_PASSWORD.toCharArray());
        }

        // Create PEM cert file
        pemCertPath = tempDir.resolve("certs.pem").toString();
        var pemContent = "-----BEGIN CERTIFICATE-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(cert.getEncoded())
                + "\n-----END CERTIFICATE-----\n";
        Files.writeString(Path.of(pemCertPath), pemContent);
    }

    @Test
    void buildSchemaResolverConfigWithNoTls() {
        var config = schemaConfig(null);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithTrustStoreNoPassword() {
        var trustStore = new TrustStore(PATH_TO_TRUSTSTORE_P12, new InlinePassword(STORE_PASS), "PKCS12");
        var tls = new Tls(null, trustStore, null, null);
        var config = schemaConfig(tls);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithTrustStoreNoType() {
        var trustStore = new TrustStore(PATH_TO_TRUSTSTORE_JKS, new InlinePassword(STORE_PASS), null);
        var tls = new Tls(null, trustStore, null, null);
        var config = schemaConfig(tls);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithInsecureTls() {
        var insecureTls = new InsecureTls(true);
        var tls = new Tls(null, insecureTls, null, null);
        var config = schemaConfig(tls);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithPlatformTrust() {
        var tls = new Tls(null, PlatformTrustProvider.INSTANCE, null, null);
        var config = schemaConfig(tls);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithNullTrustProvider() {
        var tls = new Tls(null, null, null, null);
        var config = schemaConfig(tls);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithPemTrustStore() {
        var trustStore = new TrustStore(PATH_TO_CLIENT_CERT_PEM, null, "PEM");
        var tls = new Tls(null, trustStore, null, null);
        var config = schemaConfig(tls);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithKeyThrows() {
        var tls = new Tls(new io.kroxylicious.proxy.config.tls.KeyPair("/tmp/key", "/tmp/cert", null), null, null, null);
        var config = schemaConfig(tls);
        assertThatThrownBy(() -> buildValidatorConfig(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TLS client authentication (key) is not supported");
    }

    @Test
    void buildSchemaResolverConfigWithCipherSuitesThrows() {
        var tls = new Tls(null, null, new io.kroxylicious.proxy.config.tls.AllowDeny<>(List.of("TLS_AES_128_GCM_SHA256"), null), null);
        var config = schemaConfig(tls);
        assertThatThrownBy(() -> buildValidatorConfig(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Custom cipher suites are not supported");
    }

    @Test
    void buildSchemaResolverConfigWithProtocolsThrows() {
        var tls = new Tls(null, null, null, new io.kroxylicious.proxy.config.tls.AllowDeny<>(List.of("TLSv1.2"), null));
        var config = schemaConfig(tls);
        assertThatThrownBy(() -> buildValidatorConfig(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Custom TLS protocols are not supported");
    }

    @Test
    void buildSchemaResolverConfigWithInsecureFalseFallsThrough() {
        var insecureTls = new InsecureTls(false);
        var tls = new Tls(null, insecureTls, null, null);
        var config = schemaConfig(tls);
        // InsecureTls with insecure=false doesn't match the instanceof check, so falls through to the else branch
        assertThatThrownBy(() -> buildValidatorConfig(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported TrustProvider type");
    }

    private SchemaValidationConfig schemaConfig(@Nullable Tls tls) {
        return new SchemaValidationConfig(registryUrl, 1L, null, tls, null);
    }

    private Object buildValidatorConfig(SchemaValidationConfig schemaConfig) {
        var bytebufValidation = new BytebufValidation(null, schemaConfig, null, false, true);
        var rule = new TopicMatchingRecordValidationRule(Set.of("test-topic"), null, bytebufValidation);
        var validationConfig = new ValidationConfig(List.of(rule), null);
        return ProduceRequestValidatorBuilder.build(validationConfig);
    }

    private static String getResourceLocationOnFilesystem(String resource) {
        var url = ProduceRequestValidatorBuilderTlsTest.class.getResource(resource);
        assertThat(url).isNotNull();
        return url.getFile();
    }

}
