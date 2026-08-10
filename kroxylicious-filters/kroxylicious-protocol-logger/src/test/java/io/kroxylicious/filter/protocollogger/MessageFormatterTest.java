/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
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
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class MessageFormatterTest {

    private final MessageFormatter formatter = new MessageFormatter();

    @ParameterizedTest
    @EnumSource(value = ApiKeys.class, names = {
            "SASL_AUTHENTICATE",
            "CREATE_DELEGATION_TOKEN",
            "RENEW_DELEGATION_TOKEN",
            "EXPIRE_DELEGATION_TOKEN",
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
            "RENEW_DELEGATION_TOKEN",
            "EXPIRE_DELEGATION_TOKEN",
            "ALTER_USER_SCRAM_CREDENTIALS",
            "DESCRIBE_DELEGATION_TOKEN"
    })
    void credentialBearingResponseShowsWithheldMarker(ApiKeys apiKey) {
        String output = formatter.formatResponse(apiKey, (short) 0, responseMessageFor(apiKey));
        assertThat(output).isEqualTo(MessageFormatter.BODY_WITHHELD_MESSAGE);
    }

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
    void credentialBearingRequestDoesNotLeakSecret(ApiKeys apiKey, ApiMessage message, String secret) {
        String output = formatter.formatRequest(apiKey, (short) 0, message);
        if (secret != null) {
            assertThat(output).doesNotContain(secret);
        }
        assertThat(output).isEqualTo(MessageFormatter.BODY_WITHHELD_MESSAGE);
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
    void credentialBearingResponseDoesNotLeakSecret(ApiKeys apiKey, ApiMessage message, String secret) {
        String output = formatter.formatResponse(apiKey, (short) 0, message);
        if (secret != null) {
            assertThat(output).doesNotContain(secret);
        }
        assertThat(output).isEqualTo(MessageFormatter.BODY_WITHHELD_MESSAGE);
    }

    @Test
    void nonCredentialResponseIsFormattedAsJson() {
        // Given
        MetadataResponseData response = new MetadataResponseData();

        // When
        String output = formatter.formatResponse(ApiKeys.METADATA, (short) 12, response);

        // Then
        assertThat(output).startsWith("{").contains("\"brokers\"");
    }

    private static ApiMessage requestMessageFor(ApiKeys apiKey) {
        return switch (apiKey) {
            case SASL_AUTHENTICATE -> new SaslAuthenticateRequestData()
                    .setAuthBytes("secret-credential".getBytes(StandardCharsets.UTF_8));
            case CREATE_DELEGATION_TOKEN -> new CreateDelegationTokenRequestData()
                    .setOwnerPrincipalName("owner");
            case RENEW_DELEGATION_TOKEN -> new RenewDelegationTokenRequestData()
                    .setHmac("hmac-secret".getBytes(StandardCharsets.UTF_8));
            case EXPIRE_DELEGATION_TOKEN -> new ExpireDelegationTokenRequestData()
                    .setHmac("hmac-secret".getBytes(StandardCharsets.UTF_8));
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
            case RENEW_DELEGATION_TOKEN -> new RenewDelegationTokenResponseData();
            case EXPIRE_DELEGATION_TOKEN -> new ExpireDelegationTokenResponseData();
            case ALTER_USER_SCRAM_CREDENTIALS -> new AlterUserScramCredentialsResponseData();
            case DESCRIBE_DELEGATION_TOKEN -> new DescribeDelegationTokenResponseData()
                    .setTokens(List.of(new DescribeDelegationTokenResponseData.DescribedDelegationToken()
                            .setTokenId("token-id")
                            .setHmac("hmac-secret".getBytes(StandardCharsets.UTF_8))));
            default -> throw new IllegalArgumentException("Not a credential-bearing API: " + apiKey);
        };
    }

}
