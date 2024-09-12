/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.github.benmanes.caffeine.cache.LoadingCache;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;
import io.kroxylicious.proxy.filter.oauthbearer.sasl.BackoffStrategy;

import static io.kroxylicious.test.condition.kafka.SaslAuthenticateResponseDataCondition.saslAuthenticateResponseMatching;
import static org.apache.kafka.common.protocol.Errors.ILLEGAL_SASL_STATE;
import static org.apache.kafka.common.protocol.Errors.SASL_AUTHENTICATION_FAILED;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OauthBearerValidationFilterTest {

    @Mock(strictness = LENIENT)
    private FilterContext context;

    @Mock
    private RequestFilterResultBuilder builder;

    @Mock
    private SharedOauthBearerValidationContext sharedContext;

    @Mock
    private ScheduledExecutorService executor;

    @Mock
    private LoadingCache<String, AtomicInteger> rateLimiter;

    @Mock
    private OAuthBearerValidatorCallbackHandler oauthHandler;

    @Mock
    private BackoffStrategy strategy;

    @Mock
    private SaslServer saslServer;

    private OauthBearerValidationFilter filter;

    @BeforeEach
    void init() {
        when(sharedContext.rateLimiter()).thenReturn(rateLimiter);
        when(sharedContext.oauthHandler()).thenReturn(oauthHandler);
        when(sharedContext.backoffStrategy()).thenReturn(strategy);
        filter = new OauthBearerValidationFilter(executor, sharedContext);
    }

    @Test
    void mustForwardNotOauthBearerSasl() {
        // given
        byte[] givenBytes = "just_to_compare".getBytes();
        SaslHandshakeRequestData givenHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        SaslAuthenticateRequestData givenAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(givenBytes);

        // when
        filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenHandshakeRequest, context);
        filter.onSaslAuthenticateRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenAuthenticateRequest, context);

        // then
        verify(context).forwardRequest(any(RequestHeaderData.class), eq(givenHandshakeRequest));
        verify(context).forwardRequest(any(RequestHeaderData.class), eq(givenAuthenticateRequest));
        verifyNoInteractions(executor, rateLimiter, strategy);
    }

    @Test
    void mustHandleSaslOauthBearer() throws Exception {
        // given
        byte[] givenBytes = "just_to_compare".getBytes();
        SaslHandshakeRequestData givenHandshakeRequest = new SaslHandshakeRequestData().setMechanism(OAUTHBEARER_MECHANISM);
        SaslAuthenticateRequestData givenAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(givenBytes);
        String digest = OauthBearerValidationFilter.createCacheKey(givenBytes);
        when(rateLimiter.get(digest)).thenReturn(new AtomicInteger(0));
        when(strategy.getDelay(0)).thenReturn(Duration.ZERO);
        when(saslServer.isComplete()).thenReturn(true);

        // when
        try (MockedStatic<Sasl> dummy = mockStatic(Sasl.class)) {
            dummy.when(() -> Sasl.createSaslServer(OAUTHBEARER_MECHANISM, "kafka", null, null, oauthHandler))
                 .thenReturn(saslServer);
            filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenHandshakeRequest, context);
        }
        filter.onSaslAuthenticateRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenAuthenticateRequest, context);

        // then
        verify(context).forwardRequest(any(RequestHeaderData.class), eq(givenHandshakeRequest));
        verify(context).forwardRequest(any(RequestHeaderData.class), eq(givenAuthenticateRequest));
        verifyNoInteractions(executor);
    }

    @Test
    void mustShortCircuitFailedInitSasl() {
        SaslHandshakeRequestData givenHandshakeRequest = new SaslHandshakeRequestData().setMechanism(OAUTHBEARER_MECHANISM);
        mockBuilder();

        try (MockedStatic<Sasl> dummy = mockStatic(Sasl.class)) {
            dummy.when(() -> Sasl.createSaslServer(OAUTHBEARER_MECHANISM, "kafka", null, null, oauthHandler))
                 .thenThrow(new SaslException());
            filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenHandshakeRequest, context);
        }

        verify(builder).shortCircuitResponse(assertArg(actualResponse -> {
            assertThat(actualResponse).isInstanceOf(SaslHandshakeResponseData.class);
            assertThat(((SaslHandshakeResponseData) actualResponse).errorCode()).isEqualTo(UNKNOWN_SERVER_ERROR.code());
        }));
    }

    @Test
    void mustShortCircuitHandshakeIfSaslInitiated() {
        // given
        SaslHandshakeRequestData givenHandshakeRequest = new SaslHandshakeRequestData().setMechanism(OAUTHBEARER_MECHANISM);
        mockBuilder();

        // when
        try (MockedStatic<Sasl> dummy = mockStatic(Sasl.class)) {
            dummy.when(() -> Sasl.createSaslServer(OAUTHBEARER_MECHANISM, "kafka", null, null, oauthHandler))
                 .thenReturn(saslServer);
            filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenHandshakeRequest, context);
        }
        filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), new SaslHandshakeRequestData(), context);

        // then
        verify(context).forwardRequest(any(RequestHeaderData.class), eq(givenHandshakeRequest));
        verify(builder).shortCircuitResponse(assertArg(actualResponse -> {
            assertThat(actualResponse).isInstanceOf(SaslHandshakeResponseData.class);
            assertThat(((SaslHandshakeResponseData) actualResponse).errorCode()).isEqualTo(ILLEGAL_SASL_STATE.code());
        }));
    }

    @Test
    void mustLetPassWhenAlreadyAuthenticated() {
        byte[] givenBytes = "just_to_compare".getBytes();
        SaslAuthenticateResponseData givenAuthenticateResponse = new SaslAuthenticateResponseData().setAuthBytes(givenBytes);
        SaslAuthenticateRequestData givenAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(givenBytes);

        filter.onSaslAuthenticateResponse(SaslAuthenticateResponseData.HIGHEST_SUPPORTED_VERSION, new ResponseHeaderData(), givenAuthenticateResponse, context);
        filter.onSaslAuthenticateRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenAuthenticateRequest, context);

        verify(context).forwardResponse(any(ResponseHeaderData.class), eq(givenAuthenticateResponse));
        verify(context).forwardRequest(any(RequestHeaderData.class), eq(givenAuthenticateRequest));
        verifyNoInteractions(executor, rateLimiter, strategy);
    }

    @Test
    void willShortCircuitResponseOnTokenValidationFailed() throws Exception {
        // given
        byte[] givenBytes = "just_to_compare".getBytes();
        SaslHandshakeRequestData givenHandshakeRequest = new SaslHandshakeRequestData().setMechanism(OAUTHBEARER_MECHANISM);
        SaslAuthenticateRequestData givenAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(givenBytes);
        when(saslServer.evaluateResponse(givenBytes)).thenThrow(new SaslAuthenticationException("Authentication failed"));
        mockBuilder();
        String digest = OauthBearerValidationFilter.createCacheKey(givenBytes);
        when(rateLimiter.get(digest)).thenReturn(new AtomicInteger(0));
        when(strategy.getDelay(0)).thenReturn(Duration.ZERO);

        // when
        try (MockedStatic<Sasl> dummy = mockStatic(Sasl.class)) {
            dummy.when(() -> Sasl.createSaslServer(OAUTHBEARER_MECHANISM, "kafka", null, null, oauthHandler))
                 .thenReturn(saslServer);
            filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenHandshakeRequest, context);
        }
        filter.onSaslAuthenticateRequest(
                SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(),
                givenAuthenticateRequest,
                context
        );

        // then
        verify(builder).shortCircuitResponse(assertArg(actualResponse -> {
            assertThat(actualResponse).isInstanceOf(SaslAuthenticateResponseData.class);
            assertThat(((SaslAuthenticateResponseData) actualResponse).errorCode()).isEqualTo(SASL_AUTHENTICATION_FAILED.code());
        }));
    }

    @Test
    void willShortCircuitResponseWhenSaslFailedWithoutException() throws Exception {
        // given
        byte[] givenBytes = "just_to_compare".getBytes();
        SaslHandshakeRequestData givenHandshakeRequest = new SaslHandshakeRequestData().setMechanism(OAUTHBEARER_MECHANISM);
        SaslAuthenticateRequestData givenAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(givenBytes);
        mockBuilder();
        String digest = OauthBearerValidationFilter.createCacheKey(givenBytes);
        when(rateLimiter.get(digest)).thenReturn(new AtomicInteger(0));
        when(strategy.getDelay(0)).thenReturn(Duration.ZERO);
        when(saslServer.evaluateResponse(givenBytes)).thenReturn("invalid_token".getBytes());

        // when
        try (MockedStatic<Sasl> dummy = mockStatic(Sasl.class)) {
            dummy.when(() -> Sasl.createSaslServer(OAUTHBEARER_MECHANISM, "kafka", null, null, oauthHandler))
                 .thenReturn(saslServer);
            filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenHandshakeRequest, context);
        }
        filter.onSaslAuthenticateRequest(
                SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(),
                givenAuthenticateRequest,
                context
        );

        // then
        verify(builder).shortCircuitResponse(assertArg(actualResponse -> {
            assertThat(actualResponse).isInstanceOf(SaslAuthenticateResponseData.class);
            assertThat(((SaslAuthenticateResponseData) actualResponse).errorCode()).isEqualTo(SASL_AUTHENTICATION_FAILED.code());
        }));
    }

    @Test
    void willShortCircuitAuthenticateIfNoHandshakeBefore() {
        // given
        byte[] givenBytes = "just_to_compare".getBytes();
        SaslAuthenticateRequestData givenAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(givenBytes);
        mockBuilder();

        // when
        filter.onSaslAuthenticateRequest(
                SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(),
                givenAuthenticateRequest,
                context
        );

        // then
        verify(builder).shortCircuitResponse(assertArg(actualResponse -> {
            assertThat(actualResponse).isInstanceOf(SaslAuthenticateResponseData.class);
            assertThat(((SaslAuthenticateResponseData) actualResponse).errorCode()).isEqualTo(ILLEGAL_SASL_STATE.code());
        }));
        verifyNoInteractions(executor, rateLimiter, strategy);

    }

    @Test
    void mustDelayWhenSecondFailedAuthentication() throws Exception {
        // given
        byte[] givenBytes = "just_to_compare".getBytes();
        SaslHandshakeRequestData givenHandshakeRequest = new SaslHandshakeRequestData().setMechanism(OAUTHBEARER_MECHANISM);
        SaslAuthenticateRequestData givenAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(givenBytes);
        when(saslServer.evaluateResponse(givenBytes)).thenThrow(new SaslAuthenticationException("Authentication failed"));
        mockBuilder();
        AtomicInteger attempts = new AtomicInteger(0);
        String digest = OauthBearerValidationFilter.createCacheKey(givenBytes);
        when(rateLimiter.get(digest)).thenReturn(attempts);
        when(strategy.getDelay(0)).thenReturn(Duration.ZERO);

        // when
        try (MockedStatic<Sasl> dummy = mockStatic(Sasl.class)) {
            dummy.when(() -> Sasl.createSaslServer(OAUTHBEARER_MECHANISM, "kafka", null, null, oauthHandler))
                 .thenReturn(saslServer);
            filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenHandshakeRequest, context);
        }
        filter.onSaslAuthenticateRequest(
                SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(),
                givenAuthenticateRequest,
                context
        );
        filter.onSaslAuthenticateRequest(
                SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(),
                givenAuthenticateRequest,
                context
        );

        var order = inOrder(builder);
        order.verify(builder)
             .shortCircuitResponse(argThat(arg -> saslAuthenticateResponseMatching(data -> data.errorCode() == SASL_AUTHENTICATION_FAILED.code()).matches(arg)));
        order.verify(builder)
             .shortCircuitResponse(argThat(arg -> saslAuthenticateResponseMatching(data -> data.errorCode() == ILLEGAL_SASL_STATE.code()).matches(arg)));

    }

    @Test
    void willShortCircuitResponseWhenSaslFailed() throws Exception {
        // given
        doThrow(new SaslException("SASL failed")).when(saslServer).dispose();
        byte[] givenBytes = "just_to_compare".getBytes();
        SaslHandshakeRequestData givenHandshakeRequest = new SaslHandshakeRequestData().setMechanism(OAUTHBEARER_MECHANISM);
        SaslAuthenticateRequestData givenAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(givenBytes);
        mockBuilder();
        String digest = OauthBearerValidationFilter.createCacheKey(givenBytes);
        when(rateLimiter.get(digest)).thenReturn(new AtomicInteger(0));
        when(strategy.getDelay(0)).thenReturn(Duration.ZERO);

        // when
        try (MockedStatic<Sasl> dummy = mockStatic(Sasl.class)) {
            dummy.when(() -> Sasl.createSaslServer(OAUTHBEARER_MECHANISM, "kafka", null, null, oauthHandler))
                 .thenReturn(saslServer);
            filter.onSaslHandshakeRequest(SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), givenHandshakeRequest, context);
        }
        filter.onSaslAuthenticateRequest(
                SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(),
                givenAuthenticateRequest,
                context
        );

        // then
        verify(builder).shortCircuitResponse(assertArg(actualResponse -> {
            assertThat(actualResponse).isInstanceOf(SaslAuthenticateResponseData.class);
            assertEquals(UNKNOWN_SERVER_ERROR.code(), ((SaslAuthenticateResponseData) actualResponse).errorCode());
        }));
    }

    @SuppressWarnings("unchecked")
    private void mockBuilder() {
        var closeOrTerminalStage = mock(CloseOrTerminalStage.class);
        when(closeOrTerminalStage.withCloseConnection()).thenReturn(mock(TerminalStage.class));
        when(builder.shortCircuitResponse(any())).thenReturn(closeOrTerminalStage);
        when(context.requestFilterResultBuilder()).thenReturn(builder);
    }
}
