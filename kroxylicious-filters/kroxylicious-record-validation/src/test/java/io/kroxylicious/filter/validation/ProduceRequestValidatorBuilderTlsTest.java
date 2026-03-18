/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

    private static URL registryUrl;

    @BeforeAll
    static void init() throws Exception {
        registryUrl = URI.create("http://localhost:8080").toURL();
    }

    @Test
    void buildSchemaResolverConfigWithNoTls() {
        var config = schemaConfig(null);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithTrustStore() {
        var trustStore = new TrustStore("/path/to/truststore.jks", new InlinePassword("changeit"), "JKS");
        var tls = new Tls(null, trustStore, null, null);
        var config = schemaConfig(tls);
        // Should build successfully without exception
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithTrustStoreNoPassword() {
        var trustStore = new TrustStore("/path/to/truststore.jks", null, "PKCS12");
        var tls = new Tls(null, trustStore, null, null);
        var config = schemaConfig(tls);
        var validator = buildValidatorConfig(config);
        assertThat(validator).isNotNull();
    }

    @Test
    void buildSchemaResolverConfigWithTrustStoreNoType() {
        var trustStore = new TrustStore("/path/to/truststore.jks", new InlinePassword("changeit"), null);
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
        var trustStore = new TrustStore("/path/to/certs.pem", null, "PEM");
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
}
