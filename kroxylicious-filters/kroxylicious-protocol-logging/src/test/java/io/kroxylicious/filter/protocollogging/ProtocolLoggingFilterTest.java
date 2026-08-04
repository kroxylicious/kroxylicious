/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogging;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.testing.filter.assertj.MockFilterContextAssert;
import io.kroxylicious.testing.filter.context.MockFilterContext;

import nl.altindag.log.LogCaptor;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolLoggingFilterTest {

    private final ProtocolLoggingFilter filter = new ProtocolLoggingFilter(
            EnumSet.allOf(ApiKeys.class), new MessageFormatter(Integer.MAX_VALUE), Level.DEBUG);

    private LogCaptor logCaptor;

    @BeforeEach
    void setUp() {
        logCaptor = LogCaptor.forClass(ProtocolLoggingFilter.class);
    }

    @AfterEach
    void tearDown() {
        logCaptor.close();
    }

    static Stream<Arguments> credentialBearingRequestsWithSecrets() {
        return Stream.of(
                Arguments.of(ApiKeys.SASL_AUTHENTICATE,
                        new SaslAuthenticateRequestData()
                                .setAuthBytes("SUPER_SECRET_SASL_TOKEN".getBytes(StandardCharsets.UTF_8)),
                        "SUPER_SECRET_SASL_TOKEN"),
                Arguments.of(ApiKeys.CREATE_DELEGATION_TOKEN,
                        new CreateDelegationTokenRequestData()
                                .setOwnerPrincipalName("DELEGATION_OWNER_SECRET"),
                        "DELEGATION_OWNER_SECRET"),
                Arguments.of(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
                        new AlterUserScramCredentialsRequestData()
                                .setUpsertions(List.of(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
                                        .setName("user1")
                                        .setSalt("SCRAM_SALT_SECRET".getBytes(StandardCharsets.UTF_8))
                                        .setSaltedPassword("SCRAM_PASSWORD_SECRET".getBytes(StandardCharsets.UTF_8)))),
                        "SCRAM_PASSWORD_SECRET"),
                Arguments.of(ApiKeys.DESCRIBE_DELEGATION_TOKEN,
                        new DescribeDelegationTokenRequestData(),
                        null));
    }

    @ParameterizedTest
    @MethodSource("credentialBearingRequestsWithSecrets")
    void credentialBearingRequestHasEnvelopeAndWithheldBody(ApiKeys apiKey, ApiMessage message, String secret) {
        // Given
        RequestHeaderData header = new RequestHeaderData()
                .setCorrelationId(3)
                .setClientId("producer-1");

        // When
        String output = filter.buildRequestLogMessage(apiKey, (short) 2, header, message);

        // Then
        assertThat(output)
                .startsWith("REQUEST  " + apiKey + " v2")
                .contains("corr=3")
                .contains("client=producer-1")
                .doesNotContain("session=")
                .contains(MessageFormatter.BODY_WITHHELD_MESSAGE);
        if (secret != null) {
            assertThat(output).doesNotContain(secret);
        }
    }

    static Stream<Arguments> credentialBearingResponsesWithSecrets() {
        return Stream.of(
                Arguments.of(ApiKeys.SASL_AUTHENTICATE,
                        new SaslAuthenticateResponseData()
                                .setAuthBytes("SUPER_SECRET_SASL_RESPONSE".getBytes(StandardCharsets.UTF_8)),
                        "SUPER_SECRET_SASL_RESPONSE"),
                Arguments.of(ApiKeys.CREATE_DELEGATION_TOKEN,
                        new CreateDelegationTokenResponseData()
                                .setTokenId("DELEGATION_TOKEN_ID_SECRET")
                                .setHmac("DELEGATION_HMAC_SECRET".getBytes(StandardCharsets.UTF_8)),
                        "DELEGATION_HMAC_SECRET"),
                Arguments.of(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
                        new AlterUserScramCredentialsResponseData(),
                        null),
                Arguments.of(ApiKeys.DESCRIBE_DELEGATION_TOKEN,
                        new DescribeDelegationTokenResponseData()
                                .setTokens(List.of(new DescribeDelegationTokenResponseData.DescribedDelegationToken()
                                        .setTokenId("DESCRIBE_TOKEN_ID_SECRET")
                                        .setHmac("DESCRIBE_HMAC_SECRET".getBytes(StandardCharsets.UTF_8)))),
                        "DESCRIBE_HMAC_SECRET"));
    }

    @Test
    void envelopeSurvivesTruncation() {
        // Given
        ProtocolLoggingFilter truncatingFilter = new ProtocolLoggingFilter(
                EnumSet.allOf(ApiKeys.class), new MessageFormatter(10), Level.DEBUG);
        RequestHeaderData header = new RequestHeaderData()
                .setCorrelationId(7)
                .setClientId("my-client");
        MetadataRequestData request = new MetadataRequestData();

        // When
        String output = truncatingFilter.buildRequestLogMessage(
                ApiKeys.METADATA, (short) 12, header, request);

        // Then
        assertThat(output)
                .startsWith("REQUEST  METADATA v12")
                .contains("corr=7")
                .contains("client=my-client")
                .doesNotContain("session=")
                .contains("<truncated:");
    }

    @ParameterizedTest
    @MethodSource("credentialBearingResponsesWithSecrets")
    void credentialBearingResponseHasEnvelopeAndWithheldBody(ApiKeys apiKey, ApiMessage message, String secret) {
        // Given
        ResponseHeaderData header = new ResponseHeaderData()
                .setCorrelationId(3);

        // When
        String output = filter.buildResponseLogMessage(apiKey, (short) 2, header, message);

        // Then
        assertThat(output)
                .startsWith("RESPONSE " + apiKey + " v2")
                .contains("corr=3")
                .doesNotContain("session=")
                .contains(MessageFormatter.BODY_WITHHELD_MESSAGE);
        if (secret != null) {
            assertThat(output).doesNotContain(secret);
        }
    }

    @Test
    void shouldHandleRequestReturnsTrueForConfiguredKeyWhenLevelEnabled() {
        // Given
        ProtocolLoggingFilter f = new ProtocolLoggingFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(8192), Level.DEBUG);

        // When / Then
        assertThat(f.shouldHandleRequest(ApiKeys.METADATA, (short) 12)).isTrue();
    }

    @Test
    void shouldHandleResponseReturnsTrueForConfiguredKeyWhenLevelEnabled() {
        // Given
        ProtocolLoggingFilter f = new ProtocolLoggingFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(8192), Level.DEBUG);

        // When / Then
        assertThat(f.shouldHandleResponse(ApiKeys.METADATA, (short) 12)).isTrue();
    }

    @Test
    void shouldHandleRequestReturnsFalseForUnconfiguredKey() {
        // Given
        ProtocolLoggingFilter f = new ProtocolLoggingFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(8192), Level.DEBUG);

        // When / Then
        assertThat(f.shouldHandleRequest(ApiKeys.PRODUCE, (short) 9)).isFalse();
    }

    @Test
    void shouldHandleResponseReturnsFalseForUnconfiguredKey() {
        // Given
        ProtocolLoggingFilter f = new ProtocolLoggingFilter(
                EnumSet.of(ApiKeys.METADATA), new MessageFormatter(8192), Level.DEBUG);

        // When / Then
        assertThat(f.shouldHandleResponse(ApiKeys.PRODUCE, (short) 9)).isFalse();
    }

    @Test
    void shouldHandleRequestReturnsFalseWhenLevelDisabled() {
        // Given
        ProtocolLoggingFilter f = new ProtocolLoggingFilter(
                EnumSet.allOf(ApiKeys.class), new MessageFormatter(8192), Level.TRACE);

        // When / Then
        assertThat(f.shouldHandleRequest(ApiKeys.METADATA, (short) 12)).isFalse();
    }

    @Test
    void shouldHandleResponseReturnsFalseWhenLevelDisabled() {
        // Given
        ProtocolLoggingFilter f = new ProtocolLoggingFilter(
                EnumSet.allOf(ApiKeys.class), new MessageFormatter(8192), Level.TRACE);

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

}
