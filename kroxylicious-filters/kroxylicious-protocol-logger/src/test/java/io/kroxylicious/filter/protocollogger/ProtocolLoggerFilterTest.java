/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.kroxylicious.kafka.common.message.AlterUserScramCredentialsRequestData;
import io.kroxylicious.kafka.common.message.AlterUserScramCredentialsResponseData;
import io.kroxylicious.kafka.common.message.CreateDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.CreateDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.DescribeDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.DescribeDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.ExpireDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.ExpireDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RenewDelegationTokenRequestData;
import io.kroxylicious.kafka.common.message.RenewDelegationTokenResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.slf4j.helpers.NOPLogger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.sambarker.logsquelcher.CapturedLogs;
import io.github.sambarker.logsquelcher.LogSquelcherExtension;
import io.github.sambarker.logsquelcher.LoggingEventAssert;

import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.testing.filter.assertj.MockFilterContextAssert;
import io.kroxylicious.testing.filter.context.MockFilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

@ExtendWith(LogSquelcherExtension.class)
class ProtocolLoggerFilterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ProtocolLoggerFilter filter = new ProtocolLoggerFilter(
            EnumSet.allOf(ApiKeys.class), new MessageFormatter(), Level.DEBUG,
            LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));

    static Stream<Arguments> credentialBearingRequestsWithSecrets() {
        return Stream.of(
                argumentSet("SASL_AUTHENTICATE request withholds auth bytes",
                        ApiKeys.SASL_AUTHENTICATE,
                        new SaslAuthenticateRequestData()
                                .setAuthBytes("SUPER_SECRET_SASL_TOKEN".getBytes(StandardCharsets.UTF_8)),
                        "SUPER_SECRET_SASL_TOKEN"),
                argumentSet("CREATE_DELEGATION_TOKEN request withholds owner principal",
                        ApiKeys.CREATE_DELEGATION_TOKEN,
                        new CreateDelegationTokenRequestData()
                                .setOwnerPrincipalName("DELEGATION_OWNER_SECRET"),
                        "DELEGATION_OWNER_SECRET"),
                argumentSet("RENEW_DELEGATION_TOKEN request withholds HMAC",
                        ApiKeys.RENEW_DELEGATION_TOKEN,
                        new RenewDelegationTokenRequestData()
                                .setHmac("RENEW_HMAC_SECRET".getBytes(StandardCharsets.UTF_8)),
                        "RENEW_HMAC_SECRET"),
                argumentSet("EXPIRE_DELEGATION_TOKEN request withholds HMAC",
                        ApiKeys.EXPIRE_DELEGATION_TOKEN,
                        new ExpireDelegationTokenRequestData()
                                .setHmac("EXPIRE_HMAC_SECRET".getBytes(StandardCharsets.UTF_8)),
                        "EXPIRE_HMAC_SECRET"),
                argumentSet("ALTER_USER_SCRAM_CREDENTIALS request withholds salted password",
                        ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
                        new AlterUserScramCredentialsRequestData()
                                .setUpsertions(List.of(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
                                        .setName("user1")
                                        .setSalt("SCRAM_SALT_SECRET".getBytes(StandardCharsets.UTF_8))
                                        .setSaltedPassword("SCRAM_PASSWORD_SECRET".getBytes(StandardCharsets.UTF_8)))),
                        "SCRAM_PASSWORD_SECRET"),
                argumentSet("DESCRIBE_DELEGATION_TOKEN request withholds body",
                        ApiKeys.DESCRIBE_DELEGATION_TOKEN,
                        new DescribeDelegationTokenRequestData(),
                        null));
    }

    @ParameterizedTest
    @MethodSource("credentialBearingRequestsWithSecrets")
    void credentialBearingRequestHasHeaderAndWithheldPayload(ApiKeys apiKey, ApiMessage message, String secret) throws JsonProcessingException {
        // Given
        RequestHeaderData header = new RequestHeaderData()
                .setCorrelationId(3)
                .setClientId("producer-1");

        // When
        String output = filter.buildRequestLogMessage(apiKey, (short) 2, header, message);

        // Then
        JsonNode entry = MAPPER.readTree(output);
        assertThat(MAPPER.convertValue(entry.get("header"), Map.class))
                .containsEntry("type", "REQUEST")
                .containsEntry("apiKey", apiKey.name())
                .containsEntry("apiVersion", 2)
                .containsEntry("correlationId", 3)
                .containsEntry("clientId", "producer-1")
                .doesNotContainKey("sessionId");
        assertThat(entry.get("payload").isNull()).isTrue();
        assertThat(entry.get("payloadWithheld").asText()).isEqualTo(MessageFormatter.PAYLOAD_WITHHELD_REASON);
        if (secret != null) {
            assertThat(output).doesNotContain(secret);
        }
    }

    static Stream<Arguments> credentialBearingResponsesWithSecrets() {
        return Stream.of(
                argumentSet("SASL_AUTHENTICATE response withholds auth bytes",
                        ApiKeys.SASL_AUTHENTICATE,
                        new SaslAuthenticateResponseData()
                                .setAuthBytes("SUPER_SECRET_SASL_RESPONSE".getBytes(StandardCharsets.UTF_8)),
                        "SUPER_SECRET_SASL_RESPONSE"),
                argumentSet("CREATE_DELEGATION_TOKEN response withholds HMAC",
                        ApiKeys.CREATE_DELEGATION_TOKEN,
                        new CreateDelegationTokenResponseData()
                                .setTokenId("DELEGATION_TOKEN_ID_SECRET")
                                .setHmac("DELEGATION_HMAC_SECRET".getBytes(StandardCharsets.UTF_8)),
                        "DELEGATION_HMAC_SECRET"),
                argumentSet("RENEW_DELEGATION_TOKEN response withholds body",
                        ApiKeys.RENEW_DELEGATION_TOKEN,
                        new RenewDelegationTokenResponseData(),
                        null),
                argumentSet("EXPIRE_DELEGATION_TOKEN response withholds body",
                        ApiKeys.EXPIRE_DELEGATION_TOKEN,
                        new ExpireDelegationTokenResponseData(),
                        null),
                argumentSet("ALTER_USER_SCRAM_CREDENTIALS response withholds body",
                        ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
                        new AlterUserScramCredentialsResponseData(),
                        null),
                argumentSet("DESCRIBE_DELEGATION_TOKEN response withholds HMAC",
                        ApiKeys.DESCRIBE_DELEGATION_TOKEN,
                        new DescribeDelegationTokenResponseData()
                                .setTokens(List.of(new DescribeDelegationTokenResponseData.DescribedDelegationToken()
                                        .setTokenId("DESCRIBE_TOKEN_ID_SECRET")
                                        .setHmac("DESCRIBE_HMAC_SECRET".getBytes(StandardCharsets.UTF_8)))),
                        "DESCRIBE_HMAC_SECRET"));
    }

    @ParameterizedTest
    @MethodSource("credentialBearingResponsesWithSecrets")
    void credentialBearingResponseHasHeaderAndWithheldPayload(ApiKeys apiKey, ApiMessage message, String secret) throws JsonProcessingException {
        // Given
        ResponseHeaderData header = new ResponseHeaderData()
                .setCorrelationId(3);

        // When
        String output = filter.buildResponseLogMessage(apiKey, (short) 2, header, message);

        // Then
        JsonNode entry = MAPPER.readTree(output);
        assertThat(MAPPER.convertValue(entry.get("header"), Map.class))
                .containsEntry("type", "RESPONSE")
                .containsEntry("apiKey", apiKey.name())
                .containsEntry("apiVersion", 2)
                .containsEntry("correlationId", 3)
                .doesNotContainKey("clientId")
                .doesNotContainKey("sessionId");
        assertThat(entry.get("payload").isNull()).isTrue();
        assertThat(entry.get("payloadWithheld").asText()).isEqualTo(MessageFormatter.PAYLOAD_WITHHELD_REASON);
        if (secret != null) {
            assertThat(output).doesNotContain(secret);
        }
    }

    @Test
    void requestLogMessageIsValidJsonWithHeaderAndPayload() throws JsonProcessingException {
        // Given
        RequestHeaderData header = new RequestHeaderData()
                .setCorrelationId(42)
                .setClientId("test-client");

        // When
        String output = filter.buildRequestLogMessage(ApiKeys.METADATA, (short) 13, header, new MetadataRequestData());

        // Then
        JsonNode entry = MAPPER.readTree(output);
        assertThat(MAPPER.convertValue(entry.get("header"), Map.class))
                .containsEntry("type", "REQUEST")
                .containsEntry("apiKey", "METADATA")
                .containsEntry("apiVersion", 13)
                .containsEntry("correlationId", 42)
                .containsEntry("clientId", "test-client");
        assertThat(entry.get("payload").isNull()).isFalse();
    }

    @Test
    void responseLogMessageHasApiKeyAndApiVersion() throws JsonProcessingException {
        // Given
        ResponseHeaderData header = new ResponseHeaderData()
                .setCorrelationId(42);

        // When
        String output = filter.buildResponseLogMessage(ApiKeys.METADATA, (short) 13, header, new MetadataResponseData());

        // Then
        JsonNode entry = MAPPER.readTree(output);
        assertThat(MAPPER.convertValue(entry.get("header"), Map.class))
                .containsEntry("type", "RESPONSE")
                .containsEntry("apiKey", "METADATA")
                .containsEntry("apiVersion", 13)
                .containsEntry("correlationId", 42)
                .doesNotContainKey("clientId");
        assertThat(entry.get("payload").isNull()).isFalse();
    }

    @Test
    void shouldHandleRequestReturnsTrueForConfiguredKeyWhenLevelEnabled() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));

        // When / Then
        assertThat(f.shouldHandleRequest(ApiKeys.METADATA, (short) 12)).isTrue();
    }

    @Test
    void shouldHandleResponseReturnsTrueForConfiguredKeyWhenLevelEnabled() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));

        // When / Then
        assertThat(f.shouldHandleResponse(ApiKeys.METADATA, (short) 12)).isTrue();
    }

    @Test
    void shouldHandleRequestReturnsFalseForUnconfiguredKey() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));

        // When / Then
        assertThat(f.shouldHandleRequest(ApiKeys.PRODUCE, (short) 9)).isFalse();
    }

    @Test
    void shouldHandleResponseReturnsFalseForUnconfiguredKey() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));

        // When / Then
        assertThat(f.shouldHandleResponse(ApiKeys.PRODUCE, (short) 9)).isFalse();
    }

    @Test
    void shouldHandleRequestReturnsFalseWhenLevelDisabled() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), new MessageFormatter(), Level.DEBUG,
                NOPLogger.NOP_LOGGER, new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));

        // When / Then
        assertThat(f.shouldHandleRequest(ApiKeys.METADATA, (short) 12)).isFalse();
    }

    @Test
    void shouldHandleResponseReturnsFalseWhenLevelDisabled() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), new MessageFormatter(), Level.DEBUG,
                NOPLogger.NOP_LOGGER, new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));

        // When / Then
        assertThat(f.shouldHandleResponse(ApiKeys.METADATA, (short) 12)).isFalse();
    }

    @Test
    void onRequestForwardsMessageUnchanged() {
        // Given
        RequestHeaderData header = new RequestHeaderData()
                .setCorrelationId(42)
                .setClientId("test-client");
        MetadataRequestData request = new MetadataRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        RequestFilterResult result = filter.onRequest(ApiKeys.METADATA, (short) 12, header, request, context)
                .toCompletableFuture().join();

        // Then
        MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .hasHeaderEqualTo(header)
                .hasMessageEqualTo(request);
    }

    @Test
    void onResponseForwardsMessageUnchanged() {
        // Given
        ResponseHeaderData header = new ResponseHeaderData()
                .setCorrelationId(42);
        MetadataResponseData response = new MetadataResponseData();
        MockFilterContext context = MockFilterContext.builder(header, response).build();

        // When
        ResponseFilterResult result = filter.onResponse(ApiKeys.METADATA, (short) 12, header, response, context)
                .toCompletableFuture().join();

        // Then
        MockFilterContextAssert.assertThat(result)
                .isForwardResponse()
                .hasHeaderEqualTo(header)
                .hasMessageEqualTo(response);
    }

    @Test
    void customLoggerNameEmitsUnderThatName(CapturedLogs capturedLogs) {
        // Given
        String customName = "test.custom.logger";
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(customName), new LogWarningThrottle(Clock.systemUTC(), customName));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, context).toCompletableFuture().join();

        // Then
        var allEvents = capturedLogs.logged();
        assertThat(allEvents)
                .filteredOn(e -> customName.equals(e.getLoggerName()))
                .hasSize(1)
                .first()
                .satisfies(e -> assertThat(e.getLoggerName()).isEqualTo(customName));
        assertThat(capturedLogs.logged(ProtocolLoggerFilter.class, Level.DEBUG)).isEmpty();
    }

    @Test
    void onRequestEmitsOneEntryAtConfiguredLevel(CapturedLogs capturedLogs) throws JsonProcessingException {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, context).toCompletableFuture().join();

        // Then
        var events = capturedLogs.logged(ProtocolLoggerFilter.class, Level.DEBUG);
        LoggingEventAssert.assertThat(events).hasSize(1);
        JsonNode entry = MAPPER.readTree(events.get(0).getMessage());
        assertThat(entry.path("header").path("apiKey").asText()).isEqualTo("METADATA");
    }

    @Test
    void onResponseEmitsOneEntryAtConfiguredLevel(CapturedLogs capturedLogs) throws JsonProcessingException {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(1);
        MetadataResponseData response = new MetadataResponseData();
        MockFilterContext context = MockFilterContext.builder(header, response).build();

        // When
        f.onResponse(ApiKeys.METADATA, (short) 13, header, response, context).toCompletableFuture().join();

        // Then
        var events = capturedLogs.logged(ProtocolLoggerFilter.class, Level.DEBUG);
        LoggingEventAssert.assertThat(events).hasSize(1);
        JsonNode entry = MAPPER.readTree(events.get(0).getMessage());
        assertThat(entry.path("header").path("apiKey").asText()).isEqualTo("METADATA");
    }

    @Test
    void credentialBearingRequestEmitsWithheldEntryWithoutPlantedSecret(CapturedLogs capturedLogs) throws JsonProcessingException {
        // Given
        String plantedSecret = "PLANTED_SASL_SECRET_VALUE";
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(5).setClientId("c1");
        SaslAuthenticateRequestData request = new SaslAuthenticateRequestData()
                .setAuthBytes(plantedSecret.getBytes(StandardCharsets.UTF_8));
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        f.onRequest(ApiKeys.SASL_AUTHENTICATE, (short) 2, header, request, context).toCompletableFuture().join();

        // Then
        var events = capturedLogs.logged(ProtocolLoggerFilter.class, Level.DEBUG);
        LoggingEventAssert.assertThat(events).hasSize(1);
        String message = events.get(0).getMessage();
        assertThat(message).doesNotContain(plantedSecret);
        JsonNode entry = MAPPER.readTree(message);
        assertThat(entry.path("header").path("apiKey").asText()).isEqualTo("SASL_AUTHENTICATE");
        assertThat(entry.path("payload").isNull()).isTrue();
        assertThat(entry.path("payloadWithheld").asText()).isEqualTo(MessageFormatter.PAYLOAD_WITHHELD_REASON);
    }

    @Test
    void credentialBearingResponseEmitsWithheldEntryWithoutPlantedSecret(CapturedLogs capturedLogs) throws JsonProcessingException {
        // Given
        String plantedSecret = "PLANTED_DELEGATION_HMAC_SECRET";
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(7);
        CreateDelegationTokenResponseData response = new CreateDelegationTokenResponseData()
                .setHmac(plantedSecret.getBytes(StandardCharsets.UTF_8));
        MockFilterContext context = MockFilterContext.builder(header, response).build();

        // When
        f.onResponse(ApiKeys.CREATE_DELEGATION_TOKEN, (short) 3, header, response, context).toCompletableFuture().join();

        // Then
        var events = capturedLogs.logged(ProtocolLoggerFilter.class, Level.DEBUG);
        LoggingEventAssert.assertThat(events).hasSize(1);
        String message = events.get(0).getMessage();
        assertThat(message).doesNotContain(plantedSecret);
        JsonNode entry = MAPPER.readTree(message);
        assertThat(entry.path("header").path("apiKey").asText()).isEqualTo("CREATE_DELEGATION_TOKEN");
        assertThat(entry.path("payload").isNull()).isTrue();
        assertThat(entry.path("payloadWithheld").asText()).isEqualTo(MessageFormatter.PAYLOAD_WITHHELD_REASON);
    }

    // --- Exception handling and suppression tests ---

    private static MessageFormatter throwingFormatter() {
        return new MessageFormatter() {
            @Override
            ObjectNode formatRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage message) {
                throw new RuntimeException("converter boom");
            }

            @Override
            ObjectNode formatResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage message) {
                throw new RuntimeException("converter boom");
            }
        };
    }

    private static LogWarningThrottle throwingThrottle() {
        return new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()) {
            @Override
            void onFailure(ApiKeys apiKey, short apiVersion, Exception exception) {
                throw new RuntimeException("throttle boom");
            }
        };
    }

    @Test
    void requestWhoseFormattingThrowsIsStillForwardedUnchanged() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        RequestFilterResult result = f.onRequest(ApiKeys.METADATA, (short) 13, header, request, context)
                .toCompletableFuture().join();

        // Then
        MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .hasHeaderEqualTo(header)
                .hasMessageEqualTo(request);
    }

    @Test
    void responseWhoseFormattingThrowsIsStillForwardedUnchanged() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(1);
        MetadataResponseData response = new MetadataResponseData();
        MockFilterContext context = MockFilterContext.builder(header, response).build();

        // When
        ResponseFilterResult result = f.onResponse(ApiKeys.METADATA, (short) 13, header, response, context)
                .toCompletableFuture().join();

        // Then
        MockFilterContextAssert.assertThat(result)
                .isForwardResponse()
                .hasHeaderEqualTo(header)
                .hasMessageEqualTo(response);
    }

    @Test
    void formattingFailureEmitsWarningNamingApiKey(CapturedLogs capturedLogs) {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, context).toCompletableFuture().join();

        // Then
        var warnings = capturedLogs.logged(LogWarningThrottle.class, Level.WARN);
        LoggingEventAssert.assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0).getKeyValuePairs())
                .anyMatch(kv -> "apiKey".equals(kv.key) && ApiKeys.METADATA.equals(kv.value));
        assertThat(warnings.get(0).getKeyValuePairs())
                .anyMatch(kv -> "targetLogger".equals(kv.key) && ProtocolLoggerFilter.class.getName().equals(kv.value));
    }

    @Test
    void formattingFailureWarningDoesNotAppearAsProtocolEntry(CapturedLogs capturedLogs) {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, context).toCompletableFuture().join();

        // Then
        var debugEntries = capturedLogs.logged(ProtocolLoggerFilter.class, Level.DEBUG);
        LoggingEventAssert.assertThat(debugEntries).isEmpty();
    }

    @Test
    void normalMessageEmitsEntryWithNoWarning(CapturedLogs capturedLogs) {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), new MessageFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(Clock.systemUTC(), ProtocolLoggerFilter.class.getName()));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, context).toCompletableFuture().join();

        // Then
        var debugEntries = capturedLogs.logged(ProtocolLoggerFilter.class, Level.DEBUG);
        LoggingEventAssert.assertThat(debugEntries).hasSize(1);
        var warnings = capturedLogs.logged(LogWarningThrottle.class, Level.WARN);
        LoggingEventAssert.assertThat(warnings).isEmpty();
    }

    @Test
    void firstFailureWarnsButImmediateSecondIsSuppressed(CapturedLogs capturedLogs) {
        // Given
        Clock fixedClock = Clock.fixed(java.time.Instant.ofEpochMilli(1000), java.time.ZoneOffset.UTC);
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(fixedClock, ProtocolLoggerFilter.class.getName()));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();

        // When
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, MockFilterContext.builder(header, request).build())
                .toCompletableFuture().join();
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, MockFilterContext.builder(header, request).build())
                .toCompletableFuture().join();

        // Then
        var warnings = capturedLogs.logged(LogWarningThrottle.class, Level.WARN);
        LoggingEventAssert.assertThat(warnings).hasSize(1);
    }

    @Test
    void failureAfterIntervalWarnsAgainWithSuppressedCount(CapturedLogs capturedLogs) {
        // Given
        java.time.Instant start = java.time.Instant.ofEpochMilli(1000);
        java.util.concurrent.atomic.AtomicReference<java.time.Instant> now = new java.util.concurrent.atomic.AtomicReference<>(start);
        Clock advanceableClock = new Clock() {
            @Override
            public java.time.ZoneId getZone() {
                return java.time.ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public java.time.Instant instant() {
                return now.get();
            }
        };
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(advanceableClock, ProtocolLoggerFilter.class.getName()));
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();

        // When
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, MockFilterContext.builder(header, request).build())
                .toCompletableFuture().join();
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, MockFilterContext.builder(header, request).build())
                .toCompletableFuture().join();
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, MockFilterContext.builder(header, request).build())
                .toCompletableFuture().join();
        now.set(start.plus(LogWarningThrottle.SUPPRESSION_INTERVAL));
        f.onRequest(ApiKeys.METADATA, (short) 13, header, request, MockFilterContext.builder(header, request).build())
                .toCompletableFuture().join();

        // Then
        var warnings = capturedLogs.logged(LogWarningThrottle.class, Level.WARN);
        LoggingEventAssert.assertThat(warnings).hasSize(2);
        assertThat(warnings.get(0).getKeyValuePairs())
                .anyMatch(kv -> "targetLogger".equals(kv.key) && ProtocolLoggerFilter.class.getName().equals(kv.value));
        assertThat(warnings.get(1).getKeyValuePairs())
                .anyMatch(kv -> "suppressedCount".equals(kv.key) && Long.valueOf(2).equals(kv.value));
        assertThat(warnings.get(1).getKeyValuePairs())
                .anyMatch(kv -> "targetLogger".equals(kv.key) && ProtocolLoggerFilter.class.getName().equals(kv.value));
    }

    @Test
    void failuresForDifferentApiKeysWarnIndependently(CapturedLogs capturedLogs) {
        // Given
        Clock fixedClock = Clock.fixed(java.time.Instant.ofEpochMilli(1000), java.time.ZoneOffset.UTC);
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), new LogWarningThrottle(fixedClock, ProtocolLoggerFilter.class.getName()));

        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData metadataRequest = new MetadataRequestData();
        io.kroxylicious.kafka.common.message.ProduceRequestData produceRequest = new io.kroxylicious.kafka.common.message.ProduceRequestData();

        // When
        f.onRequest(ApiKeys.METADATA, (short) 13, header, metadataRequest, MockFilterContext.builder(header, metadataRequest).build())
                .toCompletableFuture().join();
        f.onRequest(ApiKeys.PRODUCE, (short) 9, header, produceRequest, MockFilterContext.builder(header, produceRequest).build())
                .toCompletableFuture().join();

        // Then
        var warnings = capturedLogs.logged(LogWarningThrottle.class, Level.WARN);
        LoggingEventAssert.assertThat(warnings).hasSize(2);
        assertThat(warnings.get(0).getKeyValuePairs())
                .anyMatch(kv -> "apiKey".equals(kv.key) && ApiKeys.METADATA.equals(kv.value));
        assertThat(warnings.get(1).getKeyValuePairs())
                .anyMatch(kv -> "apiKey".equals(kv.key) && ApiKeys.PRODUCE.equals(kv.value));
    }

    @Test
    void requestForwardedEvenWhenBothFormatterAndThrottleThrow() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), throwingThrottle());
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c1");
        MetadataRequestData request = new MetadataRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // When
        RequestFilterResult result = f.onRequest(ApiKeys.METADATA, (short) 13, header, request, context)
                .toCompletableFuture().join();

        // Then
        MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .hasHeaderEqualTo(header)
                .hasMessageEqualTo(request);
    }

    @Test
    void responseForwardedEvenWhenBothFormatterAndThrottleThrow() {
        // Given
        ProtocolLoggerFilter f = new ProtocolLoggerFilter(
                EnumSet.allOf(ApiKeys.class), throwingFormatter(), Level.DEBUG,
                LoggerFactory.getLogger(ProtocolLoggerFilter.class), throwingThrottle());
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(1);
        MetadataResponseData response = new MetadataResponseData();
        MockFilterContext context = MockFilterContext.builder(header, response).build();

        // When
        ResponseFilterResult result = f.onResponse(ApiKeys.METADATA, (short) 13, header, response, context)
                .toCompletableFuture().join();

        // Then
        MockFilterContextAssert.assertThat(result)
                .isForwardResponse()
                .hasHeaderEqualTo(header)
                .hasMessageEqualTo(response);
    }

}
