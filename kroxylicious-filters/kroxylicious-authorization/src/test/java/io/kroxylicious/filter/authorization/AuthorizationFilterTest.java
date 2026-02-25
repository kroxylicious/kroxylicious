/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.reflect.ClassPath;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter;

import nl.altindag.log.LogCaptor;

import static java.util.EnumSet.complementOf;
import static java.util.EnumSet.copyOf;
import static java.util.EnumSet.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.when;

class AuthorizationFilterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private static final Pattern TEST_RESOURCE_FILTER = Pattern.compile("scenarios/.*\\.yaml");

    private static final LogCaptor logCaptor = LogCaptor.forClass(AuthorizationFilter.class);

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

    @BeforeEach
    void setup() {
        logCaptor.clearLogs();
    }

    @Test
    void authorizerDoesNotDeclareSupportedResourceTypes() {
        // given
        Subject subject = Subject.anonymous();
        Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
        List<Action> actions = List.of(new Action(TopicResource.ALTER_CONFIGS, "resourceA"),
                new Action(TransactionalIdResource.DESCRIBE, "resourceB"));
        AuthorizeResult result = new AuthorizeResult(subject, new ArrayList<>(actions), new ArrayList<>());
        when(mockAuthorizer.authorize(subject, actions)).thenReturn(CompletableFuture.completedFuture(result));
        when(mockAuthorizer.supportedResourceTypes()).thenReturn(Optional.empty());
        AuthorizationFilter filter = new AuthorizationFilter(mockAuthorizer);
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        when(filterContext.authenticatedSubject()).thenReturn(subject);
        // when
        CompletionStage<AuthorizeResult> authorization = filter.authorization(filterContext, actions);
        // then
        assertThat(authorization).succeedsWithin(Duration.ZERO).satisfies(authorizeResult -> {
            assertThat(authorizeResult.denied()).isEmpty();
            assertThat(authorizeResult.allowed()).containsExactlyElementsOf(actions);
        });
    }

    @Test
    void actionsForUnsupportedResourceTypes() {
        // given
        Subject subject = Subject.anonymous();
        Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
        AuthorizeResult result = new AuthorizeResult(subject, new ArrayList<>(), new ArrayList<>());
        when(mockAuthorizer.authorize(subject, List.of())).thenReturn(CompletableFuture.completedFuture(result));
        when(mockAuthorizer.supportedResourceTypes()).thenReturn(Optional.of(Set.of()));
        AuthorizationFilter filter = new AuthorizationFilter(mockAuthorizer);
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        when(filterContext.authenticatedSubject()).thenReturn(subject);
        List<Action> actions = List.of(new Action(TopicResource.ALTER_CONFIGS, "resourceA"),
                new Action(TransactionalIdResource.DESCRIBE, "resourceB"));
        // when
        CompletionStage<AuthorizeResult> authorization = filter.authorization(filterContext, actions);
        // then
        assertThat(authorization).succeedsWithin(Duration.ZERO).satisfies(authorizeResult -> {
            assertThat(authorizeResult.denied()).isEmpty();
            assertThat(authorizeResult.allowed()).containsExactlyElementsOf(actions);
        });
    }

    @Test
    void actionsForUnsupportedAndSupportedResourceTypes() {
        // given
        Subject subject = Subject.anonymous();
        Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
        Action topicAction = new Action(TopicResource.ALTER_CONFIGS, "resourceA");
        List<Action> actions = List.of(topicAction,
                new Action(TransactionalIdResource.DESCRIBE, "resourceB"));
        AuthorizeResult result = new AuthorizeResult(subject, new ArrayList<>(List.of(topicAction)), new ArrayList<>());
        when(mockAuthorizer.authorize(subject, List.of(topicAction))).thenReturn(CompletableFuture.completedFuture(result));
        when(mockAuthorizer.supportedResourceTypes()).thenReturn(Optional.of(Set.of(TopicResource.class)));
        AuthorizationFilter filter = new AuthorizationFilter(mockAuthorizer);
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        when(filterContext.authenticatedSubject()).thenReturn(subject);
        // when
        CompletionStage<AuthorizeResult> authorization = filter.authorization(filterContext, actions);
        // then
        assertThat(authorization).succeedsWithin(Duration.ZERO).satisfies(authorizeResult -> {
            assertThat(authorizeResult.denied()).isEmpty();
            assertThat(authorizeResult.allowed()).containsExactlyElementsOf(actions);
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
        CompletionStage<RequestFilterResult> stage = authorizationFilter.onRequest(apiKeys, version, requestHeader, request, context);
        ScenarioDefinition.RequestError expectedRequestError = definition.then().expectedRequestError();
        if (expectedRequestError != null) {
            assertThat(stage).failsWithin(0, TimeUnit.SECONDS).withThrowableThat()
                    .havingCause().isInstanceOf(expectedRequestError.getCauseType()).withMessage(expectedRequestError.withCauseMessage());
        }
        else {
            handleRequestForward(definition, stage, mockUpstream, subject, topicNames, authorizationFilter, apiKeys, version);
        }
        if (!mockUpstream.isFinished()) {
            throw new IllegalStateException("test has finished, but mock responses are still queued");
        }

        assertOutcomeLogged(definition.then().isExpectAuthorizationOutcomeLog());
        // we expect that any inflight state pushed during a request is always popped on the corresponding response
        // if it is non-empty then we may have a memory leak
        assertThat(authorizationFilter.inflightState())
                .describedAs("inflight state")
                .isEmpty();
    }

    private static void assertOutcomeLogged(Boolean expectOutcomeLogs) {
        if (expectOutcomeLogs) {
            assertThat(logCaptor.getLogEvents())
                    .isNotEmpty()
                    .allSatisfy(event -> {
                        if ("INFO".equalsIgnoreCase(event.getLevel())) {
                            assertThat(event.getFormattedMessage())
                                    .startsWith("DENY")
                                    .doesNotContain("[]");
                        }
                        else if ("DEBUG".equalsIgnoreCase(event.getLevel())) {
                            assertThat(event.getFormattedMessage())
                                    .containsAnyOf("ALLOW", "NON-AUTHORIZABLE")
                                    .doesNotContain("[]");
                        }
                        else {
                            // false positive `fail` is annotated `CanIgnoreReturnValue` which is not supposed to trigger the warnings
                            // noinspection ResultOfMethodCallIgnored
                            fail("unexpected event logged: %s", event.getFormattedMessage());
                        }
                    });
        }
        else {
            assertThat(logCaptor.getLogEvents()).isEmpty();
        }
    }

    private static void handleRequestForward(ScenarioDefinition definition, CompletionStage<RequestFilterResult> stage, MockUpstream mockUpstream, Subject subject,
                                             Map<Uuid, String> topicNames, AuthorizationFilter authorizationFilter, ApiKeys apiKeys, short version) {
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
                    CompletionStage<ResponseFilterResult> filterResultCompletionStage = authorizationFilter.onResponse(apiKeys, version, response.header(),
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

    @Test
    void shouldSupportApis() {
        EnumSet<ApiKeys> allVersionsSupported = of(ApiKeys.API_VERSIONS,
                ApiKeys.SASL_HANDSHAKE,
                ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.METADATA,
                ApiKeys.FIND_COORDINATOR,
                ApiKeys.ADD_OFFSETS_TO_TXN,
                ApiKeys.END_TXN,
                ApiKeys.LIST_OFFSETS,
                ApiKeys.DESCRIBE_TOPIC_PARTITIONS,
                ApiKeys.CREATE_TOPICS,
                ApiKeys.CREATE_PARTITIONS,
                ApiKeys.DELETE_TOPICS,
                ApiKeys.DELETE_RECORDS,
                ApiKeys.DESCRIBE_PRODUCERS,
                ApiKeys.OFFSET_DELETE,
                ApiKeys.OFFSET_COMMIT,
                ApiKeys.OFFSET_FOR_LEADER_EPOCH,
                ApiKeys.TXN_OFFSET_COMMIT,
                ApiKeys.JOIN_GROUP,
                ApiKeys.SYNC_GROUP,
                ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                ApiKeys.DESCRIBE_CONFIGS,
                ApiKeys.ALTER_CONFIGS,
                ApiKeys.INCREMENTAL_ALTER_CONFIGS,
                ApiKeys.CONSUMER_GROUP_DESCRIBE,
                ApiKeys.DESCRIBE_GROUPS,
                ApiKeys.DESCRIBE_TRANSACTIONS,
                ApiKeys.LIST_TRANSACTIONS);
        EnumSet<ApiKeys> someVersionsSupported = of(ApiKeys.PRODUCE,
                ApiKeys.FETCH,
                ApiKeys.OFFSET_FETCH,
                ApiKeys.ADD_PARTITIONS_TO_TXN,
                ApiKeys.INIT_PRODUCER_ID);
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

    private static EnumSet<ApiKeys> unionOf(EnumSet<ApiKeys> allVersionsSupported, EnumSet<ApiKeys> someVersionsSupported) {
        var t = copyOf(allVersionsSupported);
        t.addAll(someVersionsSupported);
        return t;
    }
}
