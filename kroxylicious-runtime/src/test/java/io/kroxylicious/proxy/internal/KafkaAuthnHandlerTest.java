/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMessages;
import org.apache.kafka.common.security.scram.internals.ScramServerCallbackHandler;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.frame.BareSaslRequest;
import io.kroxylicious.proxy.frame.BareSaslResponse;
import io.kroxylicious.proxy.frame.ByteBufAccessor;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.frame.RequestResponseState;
import io.kroxylicious.proxy.internal.KafkaAuthnHandler.SaslMechanism;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaAuthnHandlerTest {

    public static final String CLIENT_SOFTWARE_NAME = "my-test-client";
    public static final String CLIENT_SOFTWARE_VERSION = "1.0.0";
    EmbeddedChannel channel = new EmbeddedChannel();
    private final CorrelationManager correlationManager = new CorrelationManager();
    private int corrId = 0;
    private UserEventCollector userEventCollector;
    private KafkaAuthnHandler kafkaAuthnHandler;

    private void buildChannel(Map<SaslMechanism, AuthenticateCallbackHandler> mechanismHandlers) {
        channel = new EmbeddedChannel();
        kafkaAuthnHandler = new KafkaAuthnHandler(channel,
                KafkaAuthnHandler.State.START, mechanismHandlers);
        channel.pipeline().addLast(kafkaAuthnHandler);
        userEventCollector = new UserEventCollector();
        channel.pipeline().addLast(userEventCollector);
    }

    @AfterEach
    public void after() {
        channel.checkException();
    }

    static Iterable<Short> rangeClosed(short lowerInclusive, short upperInclusive) {
        Stream<Short> range = IntStream.rangeClosed(lowerInclusive, upperInclusive).boxed().map(Integer::shortValue);
        range = Stream.concat(Stream.of((Short) null), range);
        return range.collect(Collectors.toList());
    }

    static class RequestVersions {
        // apiVersionsVersion == null => omit ApiVersions request
        private final Short apiVersionsVersion;
        // saslHandshakeVersion == null => omit SaslHandshake
        private final Short saslHandshakeVersion;
        // saslAuthenticateVersion == null => use a base SASL request (no kafka header)
        private final Short saslAuthenticateVersion;

        RequestVersions(Short apiVersionsVersion, Short saslHandshakeVersion, Short saslAuthenticateVersion) {
            this.apiVersionsVersion = apiVersionsVersion;
            this.saslHandshakeVersion = saslHandshakeVersion;
            this.saslAuthenticateVersion = saslAuthenticateVersion;
        }

        boolean useBare() {
            return saslAuthenticateVersion == null;
        }

        boolean sendApiVersions() {
            return apiVersionsVersion != null;
        }

        boolean sendHandshake() {
            return saslHandshakeVersion != null;
        }

        /**
         * KIP-152 says: "the new SaslAuthenticate requests will be used only if
         * SaslHandshake v1 is used to initiate handshake."
         * however we want to test the state machine even for broker/malicious clients
         * that don't follow the spec. i.e.
         * 1. SaslHandshake v0 followed by SaslAuthenticate
         * 2. No SaslHandshake followed by SaslAuthenticate
         */
        boolean expectValidBareAuthenticateRequest() {
            return (saslHandshakeVersion == null || saslHandshakeVersion == 0);
        }

        boolean expectGssUnsupported() {
            return (saslHandshakeVersion == null && saslAuthenticateVersion == null);
        }

        boolean expectValidFramedAuthenticateRequest() {
            return saslHandshakeVersion != null && saslHandshakeVersion >= 1;
        }

        @Override
        public String toString() {
            return "RequestVersions(" +
                    "apiVersionsVersion=" + (apiVersionsVersion == null ? "omitted" : apiVersionsVersion) +
                    ", saslHandshakeVersion=" + (saslHandshakeVersion == null ? "omitted" : saslHandshakeVersion) +
                    ", saslAuthenticateVersion=" + (saslAuthenticateVersion == null ? "unframed" : "v" + saslAuthenticateVersion) +
                    ')';
        }
    }

    public static List<Object[]> apiVersions() {
        var result = new ArrayList<Object[]>();

        for (Short apiVersionsVersion : rangeClosed(ApiVersionsRequestData.LOWEST_SUPPORTED_VERSION, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION)) {
            for (Short handshakeVersion : rangeClosed(SaslHandshakeRequestData.LOWEST_SUPPORTED_VERSION, SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION)) {
                for (Short authenticateVersion : rangeClosed(SaslHandshakeRequestData.LOWEST_SUPPORTED_VERSION, SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION)) {
                    result.add(new Object[]{ new RequestVersions(apiVersionsVersion, handshakeVersion, authenticateVersion) });
                }
            }
        }
        return result;
    }

    private PlainServerCallbackHandler saslPlainCallbackHandler(String user,
                                                                String password) {
        PlainServerCallbackHandler plainServerCallbackHandler = new PlainServerCallbackHandler();
        plainServerCallbackHandler.configure(Map.of(),
                SaslMechanism.PLAIN.mechanismName(),
                List.of(new AppConfigurationEntry(PlainLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        Map.of("user_" + user, password))));
        return plainServerCallbackHandler;
    }

    private ScramServerCallbackHandler saslScramShaCallbackHandler(SaslMechanism saslMechanism,
                                                                   String configuredUser, String configuredPassword) {
        CredentialCache.Cache<ScramCredential> credentialCache = new CredentialCache.Cache<>(ScramCredential.class);
        ScramCredential credential;
        try {
            credential = new ScramFormatter(saslMechanism.scramMechanism()).generateCredential(configuredPassword, 4096);
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        credentialCache.put(configuredUser, credential);
        ScramServerCallbackHandler callbackHandler = new ScramServerCallbackHandler(credentialCache, new DelegationTokenCache(List.of(saslMechanism.mechanismName())));
        callbackHandler.configure(null, saslMechanism.mechanismName(), null);
        return callbackHandler;
    }

    private void writeRequest(short apiVersion, ApiMessage body) {
        var apiKey = ApiKeys.forId(body.apiKey());

        int downstreamCorrelationId = corrId++;

        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        RequestHeaderData header = new RequestHeaderData()
                .setRequestApiKey(apiKey.id)
                .setRequestApiVersion(apiVersion)
                .setClientId("client-id")
                .setCorrelationId(downstreamCorrelationId);
        correlationManager.putBrokerRequest(body.apiKey(), apiVersion, downstreamCorrelationId, true, new ResponseFilter() {
            @Override
            public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
                return true;
            }

            @Override
            public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {

                return null;
            }
        }, new CompletableFuture<>(), true, RequestResponseState.empty());

        channel.writeInbound(new DecodedRequestFrame<>(apiVersion, corrId, true, header, body));
    }

    private <T extends ApiMessage> T readResponse(Class<T> cls) {
        DecodedResponseFrame<?> authenticateResponseFrame = assertInstanceOf(DecodedResponseFrame.class, channel.readOutbound());
        return assertInstanceOf(cls, authenticateResponseFrame.body());
    }

    private void doSendApiVersions(Short apiVersionsVersion) {
        // ApiVersions should propagate
        ApiVersionsRequestData apiVersionsRequest = new ApiVersionsRequestData()
                .setClientSoftwareName(CLIENT_SOFTWARE_NAME)
                .setClientSoftwareVersion(CLIENT_SOFTWARE_VERSION);
        writeRequest(apiVersionsVersion, apiVersionsRequest);

        var cse = assertInstanceOf(DecodedRequestFrame.class, channel.readInbound(),
                "Expect DecodedRequestFrame");
        assertInstanceOf(ApiVersionsRequestData.class, cse.body(),
                "Expected ApiVersions request to be propagated to next handler");
        // We don't expect an ApiVersions response, because there is no handler in the pipeline
        // which will send one
    }

    private SaslHandshakeResponseData doSendHandshake(SaslMechanism saslMechanism, Short saslHandshakeVersion) {
        SaslHandshakeRequestData handshakeRequest = new SaslHandshakeRequestData()
                .setMechanism(saslMechanism.mechanismName());
        writeRequest(saslHandshakeVersion, handshakeRequest);
        var handshakeResponseBody = readResponse(SaslHandshakeResponseData.class);
        return handshakeResponseBody;
    }

    private byte[] doSendAuthenticate(RequestVersions versions,
                                      boolean expectSuccess,
                                      boolean expectException,
                                      byte[] saslBytes) {
        byte[] responseBytes;
        if (versions.useBare()) {
            var bare = new BareSaslRequest(saslBytes, true);
            if (versions.expectValidBareAuthenticateRequest()
                    && !versions.expectGssUnsupported()) {
                channel.writeInbound(bare);
                BareSaslResponse response = assertInstanceOf(BareSaslResponse.class, channel.readOutbound());
                responseBytes = response.bytes();
            }
            else {
                var msg = assertThrows(InvalidRequestException.class, () -> channel.writeInbound(bare)).getMessage();
                if (versions.expectGssUnsupported()) {
                    assertEquals("Bare SASL bytes without GSSAPI support or prior SaslHandshake", msg);
                }
                else {
                    assertEquals("Bare SASL bytes without GSSAPI support or prior SaslHandshake", msg);
                }
                responseBytes = null;
            }
        }
        else {
            SaslAuthenticateRequestData authenticateRequest = new SaslAuthenticateRequestData()
                    .setAuthBytes(saslBytes);
            if (versions.expectValidFramedAuthenticateRequest()) {
                writeRequest(versions.saslAuthenticateVersion, authenticateRequest);
                if (expectException) {
                    fail("Unexpected response");
                }
                SaslAuthenticateResponseData saslAuthenticateResponseData = readResponse(SaslAuthenticateResponseData.class);
                if (expectSuccess) {
                    assertErrorCode(Errors.NONE, saslAuthenticateResponseData.errorCode());
                }
                else {
                    assertErrorCode(Errors.SASL_AUTHENTICATION_FAILED, saslAuthenticateResponseData.errorCode());
                }
                responseBytes = saslAuthenticateResponseData.authBytes();
            }
            else {
                assertThrows(InvalidRequestException.class, () -> writeRequest(versions.saslAuthenticateVersion, authenticateRequest));
                responseBytes = null;
            }
        }
        return responseBytes;
    }

    private void doSaslPlain(
                             RequestVersions versions,
                             String configuredUser,
                             String configuredPassword,
                             String authenticatingUser,
                             String authenticatingPassword) {

        buildChannel(Map.of(
                SaslMechanism.PLAIN, saslPlainCallbackHandler(configuredUser, configuredPassword),
                SaslMechanism.SCRAM_SHA_256, saslScramShaCallbackHandler(SaslMechanism.SCRAM_SHA_256, configuredUser, configuredPassword),
                SaslMechanism.SCRAM_SHA_512, saslScramShaCallbackHandler(SaslMechanism.SCRAM_SHA_512, configuredUser, configuredPassword)));

        if (versions.sendApiVersions()) {
            // ApiVersions should propagate
            doSendApiVersions(versions.apiVersionsVersion);

            // We don't expect an ApiVersions response, because there is no handler in the pipeline
            // which will send one
        }

        // Other requests should be denied prior to successful authentication
        assertMetadataDenied();

        if (versions.sendHandshake()) {
            assertErrorCode(Errors.NONE, doSendHandshake(SaslMechanism.PLAIN, versions.saslHandshakeVersion).errorCode());
        }

        final boolean expectSuccess = configuredUser.equals(authenticatingUser)
                && configuredPassword.equals(authenticatingPassword)
                && (versions.useBare() && versions.expectValidBareAuthenticateRequest()
                        && !versions.expectGssUnsupported() || !versions.useBare() && versions.expectValidFramedAuthenticateRequest());
        // Prior to KIP-152 and the use of SaslAuthenticate responses
        // there was no way to communicate failure back to clients so the server-size
        // SASL code had the throw
        final boolean expectException = versions.useBare();
        byte[] saslBytes = (authenticatingUser + "\0" + authenticatingUser + "\0" + authenticatingPassword).getBytes(StandardCharsets.UTF_8);
        try {
            byte[] responseBytes = doSendAuthenticate(versions, expectSuccess, expectException, saslBytes);
            if (responseBytes != null) {
                assertEquals(0, responseBytes.length);
            }
        }
        catch (SaslAuthenticationException e) {
            assertTrue(expectException,
                    e + " thrown when expecting successful authentication");
            assertEquals(KafkaAuthnHandler.State.FAILED, kafkaAuthnHandler.lastSeen);
        }

        if (expectSuccess) {
            assertAuthnSuccess();
        }
        else {
            assertAuthnFailure(versions);
        }
    }

    private void doSaslScramShaAuth(
                                    SaslMechanism saslMechanism,
                                    RequestVersions versions,
                                    String configuredUser, String configuredPassword,
                                    String authenticatingUser, String authenticatingPassword)
            throws Exception {

        buildChannel(Map.of(
                SaslMechanism.PLAIN, saslPlainCallbackHandler(configuredUser, configuredPassword),
                SaslMechanism.SCRAM_SHA_256, saslScramShaCallbackHandler(SaslMechanism.SCRAM_SHA_256, configuredUser, configuredPassword),
                SaslMechanism.SCRAM_SHA_512, saslScramShaCallbackHandler(SaslMechanism.SCRAM_SHA_512, configuredUser, configuredPassword)));

        if (versions.sendApiVersions()) {
            doSendApiVersions(versions.apiVersionsVersion);
        }

        // Other requests should be denied
        assertMetadataDenied();

        if (versions.sendHandshake()) {
            assertErrorCode(Errors.NONE, doSendHandshake(saslMechanism, versions.saslHandshakeVersion).errorCode());
        }

        final boolean expectFirstMessageSuccess = configuredUser.equals(authenticatingUser)
                && (versions.useBare() && versions.expectValidBareAuthenticateRequest()
                        && !versions.expectGssUnsupported() || !versions.useBare() && versions.expectValidFramedAuthenticateRequest());

        final boolean expectSecondMessageSuccess = configuredPassword.equals(authenticatingPassword)
                && (versions.useBare() && versions.expectValidBareAuthenticateRequest()
                        && !versions.expectGssUnsupported() || !versions.useBare() && versions.expectValidFramedAuthenticateRequest());

        final boolean expectSuccess = expectFirstMessageSuccess && expectSecondMessageSuccess;
        final boolean expectException = versions.useBare();
        try {
            ScramFormatter scramFormatter = new ScramFormatter(saslMechanism.scramMechanism());
            // First authenticate
            ScramMessages.ClientFirstMessage clientFirst = new ScramMessages.ClientFirstMessage(authenticatingUser, scramFormatter.secureRandomString(), Map.of());
            byte[] saslBytes = clientFirst.toBytes();
            byte[] responseBytes = doSendAuthenticate(versions, expectFirstMessageSuccess, expectException, saslBytes);
            if (!configuredUser.equals(authenticatingUser)) {
                assertAuthnFailure(versions);
                return;
            }
            else if (responseBytes != null) {
                // assertNotEquals(0, responseBytes.length);
                ScramMessages.ServerFirstMessage serverFirstMessage = new ScramMessages.ServerFirstMessage(responseBytes);

                // Second authenticate
                byte[] passwordBytes = ScramFormatter.normalize(new String(authenticatingPassword));
                var saltedPassword = scramFormatter.hi(passwordBytes, serverFirstMessage.salt(), serverFirstMessage.iterations());
                ScramMessages.ClientFinalMessage clientFinal = new ScramMessages.ClientFinalMessage("n,,".getBytes(StandardCharsets.UTF_8), serverFirstMessage.nonce());
                byte[] clientProof = scramFormatter.clientProof(saltedPassword, clientFirst, serverFirstMessage, clientFinal);
                clientFinal.proof(clientProof);

                byte[] finalBytes = clientFinal.toBytes();
                doSendAuthenticate(versions, expectSecondMessageSuccess, expectException, finalBytes);
            }
        }
        catch (SaslAuthenticationException e) {
            assertTrue(expectException,
                    e + " thrown when expecting successful authentication");
            assertEquals(KafkaAuthnHandler.State.FAILED, kafkaAuthnHandler.lastSeen);
        }

        if (expectSuccess) {
            assertAuthnSuccess();
        }
        else {
            assertAuthnFailure(versions);
        }

    }

    private void assertAuthnFailure(RequestVersions versions) {
        assertEquals(KafkaAuthnHandler.State.FAILED, kafkaAuthnHandler.lastSeen);
        if (versions.sendHandshake()) {
            assertFalse(kafkaAuthnHandler.saslServer.isComplete());
        }

        // Event should be propagated
        assertNull(userEventCollector.readUserEvent(),
                "Unexpected authentication event");

        // Subsequent events should not be passed upstream
        MetadataRequestData metadataRequest = new MetadataRequestData();
        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, metadataRequest);
        assertNull(channel.readInbound(),
                "Expect RPC following successful authentication to be propagated");
    }

    private void assertAuthnSuccess() {
        assertEquals(KafkaAuthnHandler.State.AUTHN_SUCCESS, kafkaAuthnHandler.lastSeen);
        assertTrue(kafkaAuthnHandler.saslServer.isComplete());

        // Event should be propagated
        var ae = assertInstanceOf(AuthenticationEvent.class, userEventCollector.readUserEvent(),
                "Expect authentication event");
        assertEquals("fred", ae.authorizationId());
        assertTrue(ae.negotiatedProperties().isEmpty());
        assertNull(userEventCollector.readUserEvent(), "Expected a single authn event");

        // Subsequent events should be passed upstream
        MetadataRequestData metadataRequest = new MetadataRequestData();
        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, metadataRequest);
        var followingFrame = assertInstanceOf(DecodedRequestFrame.class, channel.readInbound(),
                "Expect RPC following successful authentication to be propagated");
        assertInstanceOf(MetadataRequestData.class, followingFrame.body());
    }

    private static void assertErrorCode(Errors error, short errorCode) {
        assertEquals(error, Errors.forCode(errorCode));
    }

    private void assertMetadataDenied() {
        MetadataRequestData metadataRequest1 = new MetadataRequestData();
        metadataRequest1.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("topic"));

        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, metadataRequest1);
        assertNull(channel.readInbound(),
                "Non-ApiVersions requests should not propagate prior to successful authn");
        MetadataResponseData metadataResponse1 = readResponse(MetadataResponseData.class);
        assertErrorCode(Errors.ILLEGAL_SASL_STATE, metadataResponse1.topics().iterator().next().errorCode());
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    void testSaslPlainSuccessfulAuth(RequestVersions versions) {
        doSaslPlain(versions,
                "fred", "foo",
                "fred", "foo");
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    void testSaslPlainWrongPassword(RequestVersions versions) {
        doSaslPlain(versions,
                "fred", "foo",
                "fred", "bar");
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    void testSaslPlainUnknownUser(RequestVersions versions) {
        doSaslPlain(versions,
                "fred", "foo",
                "bob", "foo");
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    void testSaslScramSha256SuccessfulAuth(RequestVersions versions)
            throws Exception {
        doSaslScramShaAuth(SaslMechanism.SCRAM_SHA_256, versions,
                "fred", "password",
                "fred", "password");
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    void testSaslScramSha512SuccessfulAuth(RequestVersions versions)
            throws Exception {
        doSaslScramShaAuth(SaslMechanism.SCRAM_SHA_512, versions,
                "fred", "password",
                "fred", "password");
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    void testSaslScramSha512WrongPassword(RequestVersions versions)
            throws Exception {
        doSaslScramShaAuth(SaslMechanism.SCRAM_SHA_512, versions,
                "fred", "password",
                "fred", "wrongpassword");
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    void testSaslScramSha512UnknownUser(RequestVersions versions)
            throws Exception {
        doSaslScramShaAuth(SaslMechanism.SCRAM_SHA_512, versions,
                "fred", "password",
                "bob", "password");
    }

    @Test
    void testUnknownMechanism() {
        buildChannel(Map.of(
                SaslMechanism.PLAIN, saslPlainCallbackHandler("bob", "pa55word")));
        var resp = doSendHandshake(SaslMechanism.SCRAM_SHA_256, SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION);
        assertErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM, resp.errorCode());
        assertEquals(List.of("PLAIN"), resp.mechanisms());
    }

    @Test
    void testApiVersionsAfterSuccessfulAuth() {
        buildChannel(Map.of(
                SaslMechanism.PLAIN, saslPlainCallbackHandler("fred", "foo")));
        kafkaAuthnHandler.lastSeen = KafkaAuthnHandler.State.AUTHN_SUCCESS;

        doSendApiVersions(ApiVersionsRequestData.LOWEST_SUPPORTED_VERSION);
    }

    @Test
    void testCustomRequestFrameAfterSuccessfulAuth() {
        buildChannel(Map.of(
                SaslMechanism.PLAIN, saslPlainCallbackHandler("fred", "foo")));
        kafkaAuthnHandler.lastSeen = KafkaAuthnHandler.State.AUTHN_SUCCESS;

        RequestFrame frame = new CustomRequestFrame(1);
        channel.writeInbound(frame);
        assertEquals(frame, channel.readInbound());
    }

    record CustomRequestFrame(int correlationId) implements RequestFrame {

        @Override
        public int estimateEncodedSize() {
            return 0;
        }

        @Override
        public void encode(ByteBufAccessor out) {
        }

        @Override
        public RequestResponseState requestResponseState() {
            return RequestResponseState.empty();
        }

        @Override
        public boolean decodeResponse() {
            return false;
        }
    }

    // TODO check that mechanism selection via SaslHandshake actually works
    // TODO check that unexpected state transitions are handled with disconnection
    // TODO check that unknown read type (like ProxyDecodeEvent) propagate to upstream handlers
}
