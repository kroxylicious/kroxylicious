/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.RestoreSystemProperties;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.oauthbearer.OauthBearerValidation.Config;

import static io.kroxylicious.proxy.filter.oauthbearer.OauthBearerValidation.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@RestoreSystemProperties
class OauthBearerValidationTest {

    @Mock
    private FilterFactoryContext ffc;

    @Mock
    private OAuthBearerValidatorCallbackHandler callbackHandler;

    @Mock
    private FilterDispatchExecutor executor;

    @Test
    void noArgsConstructor() {
        var validation = new OauthBearerValidation();
        assertThat(validation).isNotNull();
        validation.close(null);
    }

    @Test
    void mustProvideDefaultValuesForConfig() throws Exception {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        Config yamlConfig = yamlMapper.readerFor(Config.class).readValue("""
                jwksEndpointUrl: https://jwks.endpoint
                """);

        assertThat(yamlConfig).isEqualTo(defaultConfig());
    }

    @Test
    @SuppressWarnings("java:S5838")
    void mustInitAndCreateFilterWithDefaultsNull() {
        mustInitAndCreateFilter(defaultConfig());
    }

    @Test
    void mustInitAndCreateFilterWithNegativeAndEmptyValues() {
        Config config = new Config(
                URI.create("https://jwks.endpoint"),
                -1L,
                -1L,
                -1L,
                "",
                " ",
                -1L,
                -1L,
                null,
                null);
        mustInitAndCreateFilter(config);
    }

    @Test
    @SuppressWarnings("java:S5838")
    void mustInitAndCreateFilterFromConfig() throws Exception {
        // given
        when(ffc.filterDispatchExecutor()).thenReturn(executor);
        OauthBearerValidation oauthBearerValidation = new OauthBearerValidation(callbackHandler);
        Config config = new Config(
                new URI("https://jwks.endpoint"),
                10000L,
                20000L,
                30000L,
                "otherScope",
                "otherClaim",
                10000L,
                500L,
                "https://first.audience, https://second.audience",
                "https://issuer.endpoint");

        // when
        SharedOauthBearerValidationContext sharedContext = oauthBearerValidation.initialize(ffc, config);
        OauthBearerValidationFilter filter = oauthBearerValidation.createFilter(ffc, sharedContext);

        // then
        verify(callbackHandler).configure(
                assertArg(configMap -> {
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL)).isEqualTo("https://jwks.endpoint");
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS)).isEqualTo(10000L);
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS)).isEqualTo(20000L);
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS)).isEqualTo(30000L);
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME)).isEqualTo("otherScope");
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME)).isEqualTo("otherClaim");
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE)).isEqualTo(List.of("https://first.audience", "https://second.audience"));
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER)).isEqualTo("https://issuer.endpoint");
                }),
                eq("OAUTHBEARER"),
                anyList());
        assertThat(filter).isNotNull();
        assertThat(config.authenticateBackOffMaxMs()).isEqualTo(10000);
    }

    @Test
    void mustCloseOauthHandler() throws Exception {
        // given
        OauthBearerValidation oauthBearerValidation = new OauthBearerValidation(callbackHandler);

        // when
        oauthBearerValidation.close(oauthBearerValidation.initialize(ffc, defaultConfig()));

        // then
        verify(callbackHandler).close();
    }

    @Test
    void initShouldAddJwksEndpointToOauthAllowList() {
        // given
        OauthBearerValidation oauthBearerValidation = new OauthBearerValidation(callbackHandler);
        Config config = defaultConfig(URI.create("https://" + UUID.randomUUID() + ".invalid"));

        // when
        oauthBearerValidation.initialize(ffc, config);

        // then
        assertThat(System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG))
                .isNotEmpty()
                .contains(config.jwksEndpointUrl().toString());
    }

    static Stream<Arguments> closeShouldRestoreOauthAllowList() {
        return Stream.of(
                Arguments.argumentSet("close clears system property",
                        (Consumer<String>) unused -> System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG),
                        (BiConsumer<String, String>) (initial, propValue) -> assertThat(propValue).isNull()),
                Arguments.argumentSet("close preserves an existing system property that exactly matches config value",
                        (Consumer<String>) configValue -> System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, configValue),
                        (BiConsumer<String, String>) (initial, propValue) -> assertThat(propValue).isEqualTo(initial)),
                Arguments.argumentSet("close preserves an existing system property that did not matches config value",
                        (Consumer<String>) configValue -> System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "https://another.invalid"),
                        (BiConsumer<String, String>) (initial, propValue) -> assertThat(propValue).isEqualTo("https://another.invalid")));
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("java:S6103") // false positive, the consumer does include an assertion.
    void closeShouldRestoreOauthAllowList(Consumer<String> prepareSysProperties, BiConsumer<String, String> postCloseAssertions) {
        // given
        var jwksEndpointUrl = "https://" + UUID.randomUUID() + ".invalid";
        prepareSysProperties.accept(jwksEndpointUrl);
        OauthBearerValidation oauthBearerValidation = new OauthBearerValidation(callbackHandler);
        Config config = defaultConfig(URI.create(jwksEndpointUrl));
        var context = oauthBearerValidation.initialize(ffc, config);

        // when
        oauthBearerValidation.close(context);

        // then
        assertThat(System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG))
                .satisfies(current -> postCloseAssertions.accept(jwksEndpointUrl, current));
    }

    @SuppressWarnings("java:S5838")
    void mustInitAndCreateFilter(Config config) {
        // given
        OauthBearerValidation oauthBearerValidation = new OauthBearerValidation(callbackHandler);
        when(ffc.filterDispatchExecutor()).thenReturn(executor);

        // when
        SharedOauthBearerValidationContext sharedContext = oauthBearerValidation.initialize(ffc, config);
        OauthBearerValidationFilter filter = oauthBearerValidation.createFilter(ffc, sharedContext);

        // then
        verify(callbackHandler).configure(
                assertArg(configMap -> {
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL)).isEqualTo("https://jwks.endpoint");
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS))
                            .isEqualTo(SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS);
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS))
                            .isEqualTo(SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS);
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS))
                            .isEqualTo(SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS);
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME)).isEqualTo(SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
                    assertThat(configMap.get(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME)).isEqualTo(SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME);
                }),
                eq("OAUTHBEARER"),
                anyList());
        assertThat(filter).isNotNull();
        assertThat(sharedContext.config().authenticateBackOffMaxMs()).isEqualTo(60000);
        assertThat(sharedContext.config().authenticateCacheMaxSize()).isEqualTo(1000);
    }

    private Config defaultConfig() {
        return defaultConfig(URI.create("https://jwks.endpoint"));
    }

    private Config defaultConfig(URI jwksEndpointUrl) {
        return new Config(
                jwksEndpointUrl,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}