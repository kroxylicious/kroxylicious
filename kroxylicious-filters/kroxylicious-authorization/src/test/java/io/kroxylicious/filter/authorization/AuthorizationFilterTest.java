/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.RequestHeaderDataJsonConverter;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.ResponseHeaderDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.reflect.ClassPath;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.EnumSet.complementOf;
import static java.util.EnumSet.copyOf;
import static java.util.EnumSet.of;
import static org.assertj.core.api.Assertions.assertThat;

class AuthorizationFilterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private static final Pattern TEST_RESOURCE_FILTER = Pattern.compile("scenarios/.*\\.yaml");

    static Stream<Arguments> authorization() throws Exception {
        List<ClassPath.ResourceInfo> resources = ClassPath.from(AuthorizationFilterTest.class.getClassLoader()).getResources().stream()
                .filter(ri -> TEST_RESOURCE_FILTER.matcher(ri.getResourceName()).matches()).toList();
        return resources.stream().map(resourceInfo -> {
            try {
                ScenarioDefinition scenarioDefinition = MAPPER.reader().readValue(resourceInfo.asByteSource().read(), ScenarioDefinition.class);
                return Arguments.argumentSet(resourceInfo.getResourceName(), scenarioDefinition);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @ParameterizedTest
    @MethodSource
    void authorization(ScenarioDefinition definition) {
        SimpleAuthorizer authorizer = new SimpleAuthorizer(definition.given().authorizerRules());
        AuthorizationFilter authorizationFilter = new AuthorizationFilter(authorizer);
        ApiKeys apiKeys = definition.metadata().apiKeys();
        short version = definition.metadata().apiVersion();
        short requestHeaderVersion = apiKeys.requestHeaderVersion(version);
        MockUpstream mockUpstream = new MockUpstream(definition.given().mockedUpstreamResponses());

        ApiMessage request = KafkaApiMessageConverter.requestConverterFor(apiKeys.messageType).reader().apply(definition.when().request(), version);
        RequestHeaderData requestHeader = RequestHeaderDataJsonConverter.read(definition.when().requestHeader(), requestHeaderVersion);

        Map<Uuid, String> topicNames = Optional.ofNullable(definition.given().topicNames()).orElse(Map.of());
        Subject subject = new Subject(new User(definition.when().subject()));
        FilterContext context = new MockFilterContext(requestHeader, request, subject, topicNames, mockUpstream);
        CompletionStage<RequestFilterResult> stage = authorizationFilter.onRequest(apiKeys, requestHeader, request, context);
        RequestFilterResult actual = assertThat(stage).succeedsWithin(Duration.ZERO).actual();
        if (actual.drop()) {
            if (!mockUpstream.isFinished()) {
                throw new IllegalStateException("mock upstream still has responses queued, but request was dropped by filter");
            }
        }
        else {
            if (!actual.shortCircuitResponse()) {
                if (mockUpstream.isFinished()) {
                    throw new IllegalStateException("mock upstream has no responses queued, but filter forwarded request");
                }
                ApiMessage forwardedMessage = Objects.requireNonNull(actual.message());
                ApiMessage forwardedHeader = Objects.requireNonNull(actual.header());
                MockUpstream.Response response = mockUpstream.respond((RequestHeaderData) forwardedHeader, forwardedMessage);
                MockFilterContext responseContext = new MockFilterContext(response.header(), response.message(), subject, topicNames, mockUpstream);
                CompletionStage<ResponseFilterResult> filterResultCompletionStage = authorizationFilter.onResponse(apiKeys, response.header(), response.message(),
                        responseContext);
                ResponseFilterResult responseResult = assertThat(filterResultCompletionStage).succeedsWithin(Duration.ZERO).actual();
                if (responseResult.drop()) {
                    assertThat(definition.then().expectedResponse()).isNull();
                    assertThat(definition.then().expectedResponseHeader()).isNull();
                }
                else {
                    String actualMessage = toYaml(KafkaApiMessageConverter.responseConverterFor(apiKeys.messageType).writer().apply(responseResult.message(), version));
                    ApiMessage header = Objects.requireNonNull(responseResult.header());
                    String actualHeader = toYaml(ResponseHeaderDataJsonConverter.write((ResponseHeaderData) header, apiKeys.responseHeaderVersion(version)));
                    assertThat(actualMessage).isEqualTo(toYaml(definition.then().expectedResponse()));
                    assertThat(actualHeader).isEqualTo(toYaml(definition.then().expectedResponseHeader()));
                }
            }
            else {
                if (!mockUpstream.isFinished()) {
                    throw new IllegalStateException("mock upstream still has responses queued, but filter short circuit responded");
                }
                ApiMessage forwardedMessage = Objects.requireNonNull(actual.message());
                ApiMessage forwardedHeader = Objects.requireNonNull(actual.header());
                String actualMessage = toYaml(KafkaApiMessageConverter.responseConverterFor(apiKeys.messageType).writer().apply(forwardedMessage, version));
                String actualHeader = toYaml(ResponseHeaderDataJsonConverter.write((ResponseHeaderData) forwardedHeader, apiKeys.responseHeaderVersion(version)));
                assertThat(actualMessage).isEqualTo(toYaml(definition.then().expectedResponse()));
                assertThat(actualHeader).isEqualTo(toYaml(definition.then().expectedResponseHeader()));
            }
        }
        if (!mockUpstream.isFinished()) {
            throw new IllegalStateException("test has finished, but mock responses are still queued");
        }
    }

    private static String toYaml(Object actualBody) {
        try {
            return MAPPER.writer().writeValueAsString(actualBody);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void shouldSupportApis() {
        EnumSet<ApiKeys> allVersionsSupported = of(ApiKeys.API_VERSIONS,
                ApiKeys.SASL_HANDSHAKE,
                ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.METADATA,
                ApiKeys.FIND_COORDINATOR,
                ApiKeys.INIT_PRODUCER_ID,
                ApiKeys.ADD_OFFSETS_TO_TXN,
                ApiKeys.END_TXN);
        EnumSet<ApiKeys> someVersionsSupported = of(ApiKeys.PRODUCE);
        EnumSet<ApiKeys> noVersionsSupported = complementOf(unionOf(allVersionsSupported, someVersionsSupported));
        for (ApiKeys apiKey : allVersionsSupported) {

            assertThat(AuthorizationFilter.isApiSupported(apiKey))
                    .as(apiKey + " is supported")
                    .isTrue();

            for (short apiVersion : apiKey.allVersions()) {
                assertThat(AuthorizationFilter.isApiVersionSupported(apiKey, apiVersion))
                        .as(apiKey + " is supported @v" + apiVersion)
                        .isTrue();
            }

            assertThat(AuthorizationFilter.minSupportedApiVersion(apiKey))
                    .isEqualTo(apiKey.oldestVersion());

            assertThat(AuthorizationFilter.maxSupportedApiVersion(apiKey))
                    .isEqualTo(apiKey.latestVersion());
        }

        for (ApiKeys apiKey : noVersionsSupported) {
            assertThat(AuthorizationFilter.isApiSupported(apiKey))
                    .as(apiKey + " is not supported")
                    .isFalse();

            for (short apiVersion : apiKey.allVersions()) {
                assertThat(AuthorizationFilter.isApiVersionSupported(apiKey, apiVersion))
                        .as(apiKey + " is not supported @v" + apiVersion)
                        .isFalse();
            }

            assertThat(AuthorizationFilter.minSupportedApiVersion(apiKey))
                    .isEqualTo((short) -1);

            assertThat(AuthorizationFilter.maxSupportedApiVersion(apiKey))
                    .isEqualTo((short) -1);
        }
    }

    @NonNull
    private static EnumSet<ApiKeys> unionOf(EnumSet<ApiKeys> allVersionsSupported, EnumSet<ApiKeys> someVersionsSupported) {
        var t = copyOf(allVersionsSupported);
        t.addAll(someVersionsSupported);
        return t;
    }
}
