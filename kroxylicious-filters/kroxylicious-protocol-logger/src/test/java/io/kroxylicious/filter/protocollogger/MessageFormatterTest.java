/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.nio.charset.StandardCharsets;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class MessageFormatterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
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
    void credentialBearingRequestHasNullPayloadAndWithheldFlag(ApiKeys apiKey) {
        // Given
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c");

        // When
        ObjectNode entry = formatter.formatRequest(apiKey, (short) 0, header, requestMessageFor(apiKey));

        // Then
        assertThat(entry.get("payload").isNull()).isTrue();
        assertThat(entry.get("payloadWithheld").asText()).isEqualTo(MessageFormatter.PAYLOAD_WITHHELD_REASON);
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
    void credentialBearingResponseHasNullPayloadAndWithheldFlag(ApiKeys apiKey) {
        // Given
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(1);

        // When
        ObjectNode entry = formatter.formatResponse(apiKey, (short) 0, header, responseMessageFor(apiKey));

        // Then
        assertThat(entry.get("payload").isNull()).isTrue();
        assertThat(entry.get("payloadWithheld").asText()).isEqualTo(MessageFormatter.PAYLOAD_WITHHELD_REASON);
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
        // Given
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c");

        // When
        ObjectNode entry = formatter.formatRequest(apiKey, (short) 0, header, message);

        // Then
        assertThat(entry.get("payload").isNull()).isTrue();
        assertThat(entry.get("payloadWithheld").asText()).isEqualTo(MessageFormatter.PAYLOAD_WITHHELD_REASON);
        if (secret != null) {
            assertThat(entry.toString()).doesNotContain(secret);
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
    void credentialBearingResponseDoesNotLeakSecret(ApiKeys apiKey, ApiMessage message, String secret) {
        // Given
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(1);

        // When
        ObjectNode entry = formatter.formatResponse(apiKey, (short) 0, header, message);

        // Then
        assertThat(entry.get("payload").isNull()).isTrue();
        assertThat(entry.get("payloadWithheld").asText()).isEqualTo(MessageFormatter.PAYLOAD_WITHHELD_REASON);
        if (secret != null) {
            assertThat(entry.toString()).doesNotContain(secret);
        }
    }

    @Test
    void nonCredentialRequestHasPayloadWithContent() {
        // Given
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("producer-1");

        // When
        ObjectNode entry = formatter.formatRequest(ApiKeys.METADATA, (short) 13, header, new MetadataRequestData());

        // Then
        assertThat(entry.has("payloadWithheld")).isFalse();
        assertThat(entry.get("payload").isNull()).isFalse();
        assertThat(entry.get("payload").has("topics")).isTrue();
    }

    @Test
    void nonCredentialResponseHasPayloadWithContent() {
        // Given
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(1);

        // When
        ObjectNode entry = formatter.formatResponse(ApiKeys.METADATA, (short) 12, header, new MetadataResponseData());

        // Then
        assertThat(entry.has("payloadWithheld")).isFalse();
        assertThat(entry.get("payload").isNull()).isFalse();
        assertThat(entry.get("payload").has("brokers")).isTrue();
    }

    @Test
    void requestHeaderHasExpectedFields() {
        // Given
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(7).setClientId("my-client");

        // When
        ObjectNode entry = formatter.formatRequest(ApiKeys.METADATA, (short) 13, header, new MetadataRequestData());

        // Then
        assertThat(MAPPER.convertValue(entry.get("header"), Map.class))
                .containsEntry("type", "REQUEST")
                .containsEntry("apiKey", "METADATA")
                .containsEntry("apiVersion", 13)
                .containsEntry("correlationId", 7)
                .containsEntry("clientId", "my-client")
                .doesNotContainKey("sessionId");
    }

    @Test
    void responseHeaderHasExpectedFields() {
        // Given
        ResponseHeaderData header = new ResponseHeaderData().setCorrelationId(7);

        // When
        ObjectNode entry = formatter.formatResponse(ApiKeys.METADATA, (short) 13, header, new MetadataResponseData());

        // Then
        assertThat(MAPPER.convertValue(entry.get("header"), Map.class))
                .containsEntry("type", "RESPONSE")
                .containsEntry("apiKey", "METADATA")
                .containsEntry("apiVersion", 13)
                .containsEntry("correlationId", 7)
                .doesNotContainKey("clientId")
                .doesNotContainKey("sessionId");
    }

    @Test
    void requestAndResponseHeadersShareKeySetExceptClientId() {
        // Given
        RequestHeaderData reqHeader = new RequestHeaderData().setCorrelationId(1).setClientId("c");
        ResponseHeaderData resHeader = new ResponseHeaderData().setCorrelationId(1);

        // When
        ObjectNode reqEntry = formatter.formatRequest(ApiKeys.METADATA, (short) 13, reqHeader, new MetadataRequestData());
        ObjectNode resEntry = formatter.formatResponse(ApiKeys.METADATA, (short) 13, resHeader, new MetadataResponseData());

        // Then
        assertThat(MAPPER.convertValue(reqEntry.get("header"), Map.class))
                .containsOnlyKeys("type", "apiKey", "apiVersion", "correlationId", "clientId");
        assertThat(MAPPER.convertValue(resEntry.get("header"), Map.class))
                .containsOnlyKeys("type", "apiKey", "apiVersion", "correlationId");
    }

    @Test
    void entryIsValidJson() {
        // Given
        RequestHeaderData header = new RequestHeaderData().setCorrelationId(1).setClientId("c");

        // When
        ObjectNode entry = formatter.formatRequest(ApiKeys.METADATA, (short) 13, header, new MetadataRequestData());
        String json = MessageFormatter.prettyPrint(entry);

        // Then
        assertThat(json).startsWith("{");
        assertThat(entry.has("header")).isTrue();
        assertThat(entry.has("payload")).isTrue();
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
