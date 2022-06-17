/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.example.authn;

import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.SaslAuthenticateRequestFilter;
import io.kroxylicious.proxy.filter.SaslAuthenticateResponseFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeRequestFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeResponseFilter;

public class SaslAuthnObserver
        implements SaslHandshakeRequestFilter,
        SaslHandshakeResponseFilter,
        SaslAuthenticateRequestFilter,
        SaslAuthenticateResponseFilter {

    private String mechanism;
    private String principalName;
    private boolean authenticated;
    private long sessionLifetimeMs;

    @Override
    public void onSaslHandshakeRequest(SaslHandshakeRequestData request,
                                       KrpcFilterContext context) {
        this.mechanism = request.mechanism();
        context.forwardRequest(request);
    }

    @Override
    public void onSaslHandshakeResponse(SaslHandshakeResponseData response,
                                        KrpcFilterContext context) {
        if (response.errorCode() != Errors.NONE.code()) {
            this.mechanism = null;
        }
        context.forwardResponse(response);
    }

    @Override
    public void onSaslAuthenticateRequest(SaslAuthenticateRequestData request,
                                          KrpcFilterContext context) {
        byte[] bytes = request.authBytes();
        switch (mechanism) {
            case "PLAIN":
                principalName = "foo";
                break;
            case "SCRAM-SHA-256":
                principalName = "bar";
                break;
            case "SCRAM-SHA-512":
                principalName = "baz";
                break;
        }
        context.forwardRequest(request);
    }

    @Override
    public void onSaslAuthenticateResponse(SaslAuthenticateResponseData response,
                                           KrpcFilterContext context) {
        if (response.errorCode() == Errors.NONE.code()) {
            authenticated = true;
            sessionLifetimeMs = response.sessionLifetimeMs();
            // TODO How to propagate this state?
            // via a user event fired on the ChannelHandlerContext via the filter context?
            // or as an attribute on the channel?
        }
        // response.authBytes();
        // response.errorCode();
        // response.errorMessage();
        context.forwardResponse(response);
    }

}
