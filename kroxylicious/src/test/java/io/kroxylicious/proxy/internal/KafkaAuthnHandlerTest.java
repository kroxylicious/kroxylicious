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

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.frame.BareSaslRequest;
import io.kroxylicious.proxy.frame.BareSaslResponse;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.future.PromiseImpl;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAuthnHandlerTest {

    public static final String CLIENT_SOFTWARE_NAME = "my-test-client";
    public static final String CLIENT_SOFTWARE_VERSION = "1.0.0";
    EmbeddedChannel channel = new EmbeddedChannel();
    private final CorrelationManager correlationManager = new CorrelationManager();
    private int corrId = 0;
    private UserEventCollector userEventCollector;
    private KafkaAuthnHandler kafkaAuthnHandler;

    private void buildChanneel(Map<KafkaAuthnHandler.SaslMechanism, AuthenticateCallbackHandler> mechanismHandlers) {
        channel = new EmbeddedChannel();
        kafkaAuthnHandler = new KafkaAuthnHandler(
                KafkaAuthnHandler.State.START, mechanismHandlers);
        channel.pipeline().addLast(kafkaAuthnHandler);
        userEventCollector = new UserEventCollector();
        channel.pipeline().addLast(userEventCollector);
    }

    @AfterEach
    public void after() {
        channel.checkException();
    }

    public static List<Object[]> apiVersions() {
        var result = new ArrayList<Object[]>();
        for (short apiVersionsVersion = ApiVersionsRequestData.LOWEST_SUPPORTED_VERSION; apiVersionsVersion <= ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION; apiVersionsVersion++) {
            for (short handshakeVersion = SaslHandshakeRequestData.LOWEST_SUPPORTED_VERSION; handshakeVersion <= SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION; handshakeVersion++) {
                for (short authenticateVersion = SaslHandshakeRequestData.LOWEST_SUPPORTED_VERSION; authenticateVersion <= SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION; authenticateVersion++) {
                    result.add(new Object[]{ apiVersionsVersion, handshakeVersion, authenticateVersion });
                }
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    public void testSuccessfulSaslPlainAuth(
                                            short apiVersionsVersion,
                                            short saslHandshakeVersion,
                                            short saslAuthenticateVersion) {

        buildChanneel(Map.of(KafkaAuthnHandler.SaslMechanism.PLAIN, saslPlainCallbackHandler()));

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

        // Other requests should be denied
        MetadataRequestData metadataRequest1 = new MetadataRequestData();
        metadataRequest1.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("topic"));

        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, metadataRequest1);
        assertNull(channel.readInbound(),
                "Non-ApiVersions equests should not propagate prior to successful authn");
        MetadataResponseData metadataResponse1 = readResponse(MetadataResponseData.class);
        assertEquals(Errors.ILLEGAL_SASL_STATE.code(), metadataResponse1.topics().iterator().next().errorCode());

        SaslHandshakeRequestData handshakeRequest = new SaslHandshakeRequestData()
                .setMechanism("PLAIN");
        writeRequest(saslHandshakeVersion, handshakeRequest);
        var handshakeResponseBody = readResponse(SaslHandshakeResponseData.class);
        assertEquals(Errors.NONE.code(), handshakeResponseBody.errorCode());

        if (saslHandshakeVersion == 0) {
            var bare = new BareSaslRequest("fred\0fred\0foo".getBytes(StandardCharsets.UTF_8), true);
            channel.writeInbound(bare);
            BareSaslResponse response = assertInstanceOf(BareSaslResponse.class, channel.readOutbound());
            assertEquals(0, response.bytes().length);
        }
        else {
            SaslAuthenticateRequestData authenticateRequest = new SaslAuthenticateRequestData()
                    .setAuthBytes("fred\0fred\0foo".getBytes(StandardCharsets.UTF_8));
            writeRequest(saslAuthenticateVersion, authenticateRequest);
            SaslAuthenticateResponseData saslAuthenticateResponseData = readResponse(SaslAuthenticateResponseData.class);
            assertEquals(Errors.NONE.code(), saslAuthenticateResponseData.errorCode());
        }

        // SASL server should be complete
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

    private PlainServerCallbackHandler saslPlainCallbackHandler() {
        PlainServerCallbackHandler plainServerCallbackHandler = new PlainServerCallbackHandler();
        plainServerCallbackHandler.configure(Map.of(),
                KafkaAuthnHandler.SaslMechanism.PLAIN.mechanismName(),
                List.of(new AppConfigurationEntry(PlainLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        Map.of("user_fred", "foo"))));
        return plainServerCallbackHandler;
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    public void testSuccessfulSaslScramSha256Auth(
                                                  short apiVersionsVersion,
                                                  short saslHandshakeVersion,
                                                  short saslAuthenticateVersion)
            throws Exception {
        successfulSaslScramShaAuth(KafkaAuthnHandler.SaslMechanism.SCRAM_SHA_256, apiVersionsVersion, saslHandshakeVersion, saslAuthenticateVersion);
    }

    @ParameterizedTest
    @MethodSource("apiVersions")
    public void testSuccessfulSaslScramSha512Auth(
                                                  short apiVersionsVersion,
                                                  short saslHandshakeVersion,
                                                  short saslAuthenticateVersion)
            throws Exception {
        successfulSaslScramShaAuth(KafkaAuthnHandler.SaslMechanism.SCRAM_SHA_512, apiVersionsVersion, saslHandshakeVersion, saslAuthenticateVersion);
    }

    private void successfulSaslScramShaAuth(
                                            KafkaAuthnHandler.SaslMechanism saslMechanism,
                                            short apiVersionsVersion,
                                            short saslHandshakeVersion,
                                            short saslAuthenticateVersion)
            throws Exception {

        buildChanneel(Map.of(saslMechanism, saslScramShaCallbackHandler(saslMechanism)));

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

        // Other requests should be denied
        MetadataRequestData metadataRequest1 = new MetadataRequestData();
        metadataRequest1.topics().add(new MetadataRequestData.MetadataRequestTopic().setName("topic"));

        writeRequest(MetadataRequestData.HIGHEST_SUPPORTED_VERSION, metadataRequest1);
        assertNull(channel.readInbound(),
                "Non-ApiVersions equests should not propagate prior to successful authn");
        MetadataResponseData metadataResponse1 = readResponse(MetadataResponseData.class);
        assertEquals(Errors.ILLEGAL_SASL_STATE.code(), metadataResponse1.topics().iterator().next().errorCode());

        SaslHandshakeRequestData handshakeRequest = new SaslHandshakeRequestData()
                .setMechanism(saslMechanism.mechanismName());
        writeRequest(saslHandshakeVersion, handshakeRequest);
        var handshakeResponseBody = readResponse(SaslHandshakeResponseData.class);
        assertEquals(Errors.NONE.code(), handshakeResponseBody.errorCode());

        ScramFormatter scramFormatter = new ScramFormatter(saslMechanism.scramMechanism());

        // First authenticate
        ScramMessages.ClientFirstMessage clientFirst = new ScramMessages.ClientFirstMessage("fred", scramFormatter.secureRandomString(), Map.of());
        ScramMessages.ServerFirstMessage serverFirstMessage;
        if (saslHandshakeVersion == 0) {
            var bare = new BareSaslRequest(clientFirst.toBytes(), true);
            channel.writeInbound(bare);
            BareSaslResponse response = assertInstanceOf(BareSaslResponse.class, channel.readOutbound());
            assertNotEquals(0, response.bytes().length);
            serverFirstMessage = new ScramMessages.ServerFirstMessage(response.bytes());
        }
        else {
            SaslAuthenticateRequestData authenticateRequest = new SaslAuthenticateRequestData()
                    .setAuthBytes(clientFirst.toBytes());
            writeRequest(saslAuthenticateVersion, authenticateRequest);
            SaslAuthenticateResponseData saslAuthenticateResponseData = readResponse(SaslAuthenticateResponseData.class);
            assertEquals(Errors.NONE.code(), saslAuthenticateResponseData.errorCode());
            serverFirstMessage = new ScramMessages.ServerFirstMessage(saslAuthenticateResponseData.authBytes());
        }

        // Second authenticate
        byte[] passwordBytes = ScramFormatter.normalize(new String("password"));
        var saltedPassword = scramFormatter.hi(passwordBytes, serverFirstMessage.salt(), serverFirstMessage.iterations());
        ScramMessages.ClientFinalMessage clientFinal = new ScramMessages.ClientFinalMessage("n,,".getBytes(StandardCharsets.UTF_8), serverFirstMessage.nonce());
        byte[] clientProof = scramFormatter.clientProof(saltedPassword, clientFirst, serverFirstMessage, clientFinal);
        clientFinal.proof(clientProof);

        if (saslHandshakeVersion == 0) {
            var bare = new BareSaslRequest(clientFinal.toBytes(), true);
            channel.writeInbound(bare);
            BareSaslResponse response = assertInstanceOf(BareSaslResponse.class, channel.readOutbound());
            assertNotEquals(0, response.bytes().length);
        }
        else {
            SaslAuthenticateRequestData authenticateRequest = new SaslAuthenticateRequestData()
                    .setAuthBytes(clientFinal.toBytes());
            writeRequest(saslAuthenticateVersion, authenticateRequest);
            SaslAuthenticateResponseData saslAuthenticateResponseData = readResponse(SaslAuthenticateResponseData.class);
            assertEquals(Errors.NONE.code(), saslAuthenticateResponseData.errorCode());
        }

        // TODO assertions on the final server response

        // SASL server should be complete
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

    private ScramServerCallbackHandler saslScramShaCallbackHandler(KafkaAuthnHandler.SaslMechanism saslMechanism) {
        CredentialCache.Cache<ScramCredential> credentialCache = new CredentialCache.Cache<>(ScramCredential.class);
        ScramCredential credential;
        try {
            credential = new ScramFormatter(saslMechanism.scramMechanism()).generateCredential("password", 4096);
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        credentialCache.put("fred", credential);
        ScramServerCallbackHandler callbackHandler = new ScramServerCallbackHandler(credentialCache, new DelegationTokenCache(List.of(saslMechanism.mechanismName())));
        callbackHandler.configure(null, saslMechanism.mechanismName(), null);
        return callbackHandler;
    }

    private <T extends ApiMessage> T readResponse(Class<T> cls) {
        DecodedResponseFrame<?> authenticateResponseFrame = assertInstanceOf(DecodedResponseFrame.class, channel.readOutbound());
        return assertInstanceOf(cls, authenticateResponseFrame.body());
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

        correlationManager.putBrokerRequest(body.apiKey(), apiVersion, downstreamCorrelationId, true, new KrpcFilter() {
            @Override
            public void onRequest(DecodedRequestFrame<?> decodedFrame, KrpcFilterContext filterContext) {

            }

            @Override
            public void onResponse(DecodedResponseFrame<?> decodedFrame, KrpcFilterContext filterContext) {

            }

            @Override
            public boolean shouldDeserializeResponse(ApiKeys apiKey, short apiVersion) {
                return true;
            }
        }, new PromiseImpl<>(), true);

        channel.writeInbound(new DecodedRequestFrame<>(apiVersion, corrId, true, header, body));
    }

    // TODO check the unsuccessful authentication case
    // TODO check the bad password case
    // TODO check the scram sha mechanisms
    // TODO check that unexpected state transitions are handled with disconnection
    // TODO check that unknown read type (like ProxyDecodeEvent) propagate to upstream handlers
}
