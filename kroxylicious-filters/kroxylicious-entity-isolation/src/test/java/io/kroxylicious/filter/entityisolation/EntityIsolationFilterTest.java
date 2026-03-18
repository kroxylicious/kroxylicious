/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.RequestHeaderDataJsonConverter;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.ResponseHeaderDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

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

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class EntityIsolationFilterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private static final String PACKAGE_PATH = EntityIsolationFilterTest.class.getPackageName().replace(".", "/");
    private static final Pattern ISOLATION_TEST_FILTER = Pattern.compile(String.format("%s/[A-Z_]+/\\d+/.*\\.yaml", PACKAGE_PATH));
    private static final Pattern NON_SPECIFIC_TEST_FILTER = Pattern.compile(String.format("%s/_NON-API-SPECIFIC/.*\\.yaml", PACKAGE_PATH));

    static Stream<Arguments> isolationScenarios() {
        return buildScenarios(ISOLATION_TEST_FILTER);
    }

    static Stream<Arguments> nonApiSpecificScenarios() {
        return buildScenarios(NON_SPECIFIC_TEST_FILTER);
    }

    private static Stream<Arguments> buildScenarios(Pattern testFilter) {
        try {
            List<ClassPath.ResourceInfo> resources = ClassPath.from(EntityIsolationFilterTest.class.getClassLoader()).getResources().stream()
                    .filter(ri -> testFilter.matcher(ri.getResourceName()).matches()).toList();
            return resources.stream()
                    .map(resourceInfo -> {
                        try {
                            ScenarioDefinition scenarioDefinition = MAPPER.reader().readValue(resourceInfo.asByteSource().read(), ScenarioDefinition.class);
                            return Arguments.argumentSet(resourceInfo.getResourceName(), scenarioDefinition);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException("Failed to unmarshall" + resourceInfo, e);
                        }
                    });
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to build scenarios for " + testFilter, e);
        }
    }

    private static EntityNameMapper createEntityMapper() {
        return new PrincipalEntityNameMapper(User.class, "-");
    }

    @ParameterizedTest
    @MethodSource("isolationScenarios")
    void shouldIsolate(ScenarioDefinition definition) {
        runScenario(definition);
    }

    @ParameterizedTest
    @MethodSource("nonApiSpecificScenarios")
    void shouldHandle(ScenarioDefinition definition) {
        runScenario(definition);
    }

    private void runScenario(ScenarioDefinition definition) {
        var entityMapper = createEntityMapper();
        var isolationFilter = new EntityIsolationFilter(definition.given().entityTypes(), entityMapper);
        ApiKeys apiKeys = definition.metadata().apiKeys();
        short version = definition.metadata().apiVersion();
        short requestHeaderVersion = apiKeys.requestHeaderVersion(version);
        MockUpstream mockUpstream = new MockUpstream(definition.given().mockedUpstreamResponses());

        ApiMessage request = KafkaApiMessageConverter.requestConverterFor(apiKeys.messageType).reader().apply(definition.when().request(), version);
        RequestHeaderData requestHeader = RequestHeaderDataJsonConverter.read(definition.when().requestHeader(), requestHeaderVersion);

        Map<Uuid, String> topicNames = Optional.ofNullable(definition.given().topicNames()).orElse(Map.of());
        Subject subject = new Subject(new User(definition.when().subject()));
        FilterContext context = new MockFilterContext(requestHeader, request, subject, topicNames, mockUpstream);
        CompletionStage<RequestFilterResult> stage = isolationFilter.onRequest(apiKeys, version, requestHeader, request, context);
        ScenarioDefinition.RequestError expectedRequestError = definition.then().expectedRequestError();
        if (expectedRequestError != null) {
            assertThat(stage).failsWithin(0, TimeUnit.SECONDS)
                    .withThrowableThat()
                    .havingCause().isInstanceOf(expectedRequestError.getCauseType())
                    .withMessage(expectedRequestError.withCauseMessage());
        }
        else {
            handleRequestForward(definition, stage, mockUpstream, subject, topicNames, isolationFilter, apiKeys, version);
        }
        if (!mockUpstream.isFinished()) {
            throw new IllegalStateException("test has finished, but mock responses are still queued");
        }

        assertThat(isolationFilter.getCorrelatedRequestContextMap())
                .isEmpty();
    }

    private static void handleRequestForward(ScenarioDefinition definition, CompletionStage<RequestFilterResult> stage, MockUpstream mockUpstream, Subject subject,
                                             Map<Uuid, String> topicNames, EntityIsolationFilter isolationFilter, ApiKeys apiKeys, short version) {
        RequestFilterResult actual = assertThat(stage).succeedsWithin(Duration.ZERO).actual();
        if (actual.drop()) {
            if (!mockUpstream.isFinished()) {
                throw new IllegalStateException("mock upstream still has responses queued, but request was dropped by filter");
            }
            assertThat(definition.then().isExpectRequestDropped()).isTrue();
        }
        else {
            if (!actual.shortCircuitResponse()) {
                if (mockUpstream.isFinished()) {
                    throw new IllegalStateException("mock upstream has no responses queued, but filter forwarded request");
                }
                ApiMessage forwardedMessage = Objects.requireNonNull(actual.message());
                ApiMessage forwardedHeader = Objects.requireNonNull(actual.header());
                MockUpstream.Response response = mockUpstream.respond((RequestHeaderData) forwardedHeader, forwardedMessage);
                if (definition.then().getHasResponse()) {
                    MockFilterContext responseContext = new MockFilterContext(response.header(), response.message(), subject, topicNames, mockUpstream);
                    CompletionStage<ResponseFilterResult> filterResultCompletionStage = isolationFilter.onResponse(apiKeys, version, response.header(),
                            response.message(),
                            responseContext);
                    ResponseFilterResult responseResult = assertThat(filterResultCompletionStage).succeedsWithin(Duration.ZERO).actual();
                    if (responseResult.drop()) {
                        assertThat(definition.then().expectedResponse()).isNull();
                        assertThat(definition.then().expectedResponseHeader()).isNull();
                    }
                    else {
                        String actualMessage = toYaml(
                                KafkaApiMessageConverter.responseConverterFor(apiKeys.messageType).writer().apply(responseResult.message(), version));
                        ApiMessage header = Objects.requireNonNull(responseResult.header());
                        String actualHeader = toYaml(ResponseHeaderDataJsonConverter.write((ResponseHeaderData) header, apiKeys.responseHeaderVersion(version)));
                        assertThat(actualMessage)
                                .as("Body of response to client")
                                .isEqualTo(toYaml(definition.then().expectedResponse()));
                        assertThat(actualHeader)
                                .as("Header of response to client")
                                .isEqualTo(toYaml(definition.then().expectedResponseHeader()));
                    }
                }
                else {
                    assertThat(response).isNull();
                }
            }
            else {
                if (!mockUpstream.isFinished()) {
                    throw new IllegalStateException("mock upstream still has responses queued, but filter short circuit responded");
                }
                if (definition.then().expectedErrorResponse() != null) {
                    assertThat(actual).isInstanceOfSatisfying(MockFilterContext.ErrorRequestFilterResult.class,
                            result -> assertThat(result.apiException()).isEqualTo(definition.then().expectedErrorResponse().exception()));
                }
                else {
                    assertThat(actual)
                            .withFailMessage("request unexpectedly resulted in a short-circuit error response")
                            .isNotInstanceOf(MockFilterContext.ErrorRequestFilterResult.class);
                    if (definition.then().expectedResponseHeader() != null) {
                        ApiMessage forwardedHeader = Objects.requireNonNull(actual.header());
                        String actualHeader = toYaml(
                                ResponseHeaderDataJsonConverter.write((ResponseHeaderData) forwardedHeader, apiKeys.responseHeaderVersion(version)));
                        assertThat(actualHeader).isEqualTo(toYaml(definition.then().expectedResponseHeader()));
                    }
                    if (definition.then().expectedResponse() != null) {
                        ApiMessage forwardedMessage = Objects.requireNonNull(actual.message());
                        String actualMessage = toYaml(KafkaApiMessageConverter.responseConverterFor(apiKeys.messageType).writer().apply(forwardedMessage, version));
                        assertThat(actualMessage).isEqualTo(toYaml(definition.then().expectedResponse()));
                    }
                }
            }
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
}
