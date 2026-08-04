/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogging;

import java.nio.charset.StandardCharsets;
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
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class MessageFormatterTest {

    private final MessageFormatter formatter = new MessageFormatter(Integer.MAX_VALUE);

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {
            "SASL_AUTHENTICATE",
            "CREATE_DELEGATION_TOKEN",
            "ALTER_USER_SCRAM_CREDENTIALS",
            "DESCRIBE_DELEGATION_TOKEN"
    })
    void credentialBearingRequestShowsWithheldMarker(ApiKeys apiKey) {
        String output = formatter.formatRequest(apiKey, (short) 0, requestMessageFor(apiKey));
        assertThat(output).isEqualTo(MessageFormatter.BODY_WITHHELD_MESSAGE);
    }

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {
            "SASL_AUTHENTICATE",
            "CREATE_DELEGATION_TOKEN",
            "ALTER_USER_SCRAM_CREDENTIALS",
            "DESCRIBE_DELEGATION_TOKEN"
    })
    void credentialBearingResponseShowsWithheldMarker(ApiKeys apiKey) {
        String output = formatter.formatResponse(apiKey, (short) 0, responseMessageFor(apiKey));
        assertThat(output).isEqualTo(MessageFormatter.BODY_WITHHELD_MESSAGE);
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
    void credentialBearingRequestDoesNotLeakSecret(ApiKeys apiKey, ApiMessage message, String secret) {
        String output = formatter.formatRequest(apiKey, (short) 0, message);
        if (secret != null) {
            assertThat(output).doesNotContain(secret);
        }
        assertThat(output).isEqualTo(MessageFormatter.BODY_WITHHELD_MESSAGE);
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

    @ParameterizedTest
    @MethodSource("credentialBearingResponsesWithSecrets")
    void credentialBearingResponseDoesNotLeakSecret(ApiKeys apiKey, ApiMessage message, String secret) {
        String output = formatter.formatResponse(apiKey, (short) 0, message);
        if (secret != null) {
            assertThat(output).doesNotContain(secret);
        }
        assertThat(output).isEqualTo(MessageFormatter.BODY_WITHHELD_MESSAGE);
    }

    @Test
    void bodyUnderLimitIsUnmodified() {
        // Given
        MessageFormatter unlimited = new MessageFormatter(Integer.MAX_VALUE);
        MessageFormatter limited = new MessageFormatter(8192);
        MetadataRequestData request = new MetadataRequestData();

        // When
        String full = unlimited.formatRequest(ApiKeys.METADATA, (short) 12, request);
        String truncated = limited.formatRequest(ApiKeys.METADATA, (short) 12, request);

        // Then
        assertThat(full.length()).isLessThanOrEqualTo(8192);
        assertThat(truncated).isEqualTo(full);
    }

    @Test
    void bodyOverLimitIsTruncatedWithMarker() {
        // Given
        MessageFormatter limited = new MessageFormatter(10);
        MetadataRequestData request = new MetadataRequestData();

        // When
        String output = limited.formatRequest(ApiKeys.METADATA, (short) 12, request);

        // Then
        assertThat(output)
                .contains("<truncated:")
                .contains("more chars>")
                .hasSizeLessThan(
                        new MessageFormatter(Integer.MAX_VALUE).formatRequest(ApiKeys.METADATA, (short) 12, request).length());
    }

    @Test
    void nonCredentialResponseIsFormattedAsJson() {
        // Given
        MessageFormatter unlimited = new MessageFormatter(Integer.MAX_VALUE);
        MetadataResponseData response = new MetadataResponseData();

        // When
        String output = unlimited.formatResponse(ApiKeys.METADATA, (short) 12, response);

        // Then
        assertThat(output).startsWith("{").contains("\"brokers\"");
    }

    @Test
    void withheldMarkerIsNeverTruncated() {
        // Given
        MessageFormatter limited = new MessageFormatter(1);

        // When
        String output = limited.formatRequest(ApiKeys.SASL_AUTHENTICATE, (short) 0,
                new SaslAuthenticateRequestData().setAuthBytes("secret".getBytes(StandardCharsets.UTF_8)));

        // Then
        assertThat(output).isEqualTo(MessageFormatter.BODY_WITHHELD_MESSAGE);
    }

    private static ApiMessage requestMessageFor(ApiKeys apiKey) {
        return switch (apiKey) {
            case SASL_AUTHENTICATE -> new SaslAuthenticateRequestData()
                    .setAuthBytes("secret-credential".getBytes(StandardCharsets.UTF_8));
            case CREATE_DELEGATION_TOKEN -> new CreateDelegationTokenRequestData()
                    .setOwnerPrincipalName("owner");
            case ALTER_USER_SCRAM_CREDENTIALS -> new AlterUserScramCredentialsRequestData()
                    .setUpsertions(List.of(new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion()
                            .setName("user")
                            .setSalt("salt".getBytes(StandardCharsets.UTF_8))
                            .setSaltedPassword("salted-password".getBytes(StandardCharsets.UTF_8))));
            case DESCRIBE_DELEGATION_TOKEN -> new DescribeDelegationTokenRequestData();
            default -> throw new IllegalArgumentException("Not a credential-bearing API: " + apiKey);
        };
    }

    private static ApiMessage responseMessageFor(ApiKeys apiKey) {
        return switch (apiKey) {
            case SASL_AUTHENTICATE -> new SaslAuthenticateResponseData()
                    .setAuthBytes("secret-response".getBytes(StandardCharsets.UTF_8));
            case CREATE_DELEGATION_TOKEN -> new CreateDelegationTokenResponseData()
                    .setTokenId("token-id")
                    .setHmac("hmac-secret".getBytes(StandardCharsets.UTF_8));
            case ALTER_USER_SCRAM_CREDENTIALS -> new AlterUserScramCredentialsResponseData();
            case DESCRIBE_DELEGATION_TOKEN -> new DescribeDelegationTokenResponseData()
                    .setTokens(List.of(new DescribeDelegationTokenResponseData.DescribedDelegationToken()
                            .setTokenId("token-id")
                            .setHmac("hmac-secret".getBytes(StandardCharsets.UTF_8))));
            default -> throw new IllegalArgumentException("Not a credential-bearing API: " + apiKey);
        };
    }

}
