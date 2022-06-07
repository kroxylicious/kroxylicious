/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kroxylicious.proxy.internal.filter.authn;

import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.KrpcFilterState;
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
    public KrpcFilterState onSaslHandshakeRequest(SaslHandshakeRequestData request,
                                                  KrpcFilterContext context) {
        this.mechanism = request.mechanism();
        return KrpcFilterState.FORWARD;
    }

    @Override
    public KrpcFilterState onSaslHandshakeResponse(SaslHandshakeResponseData response,
                                                   KrpcFilterContext context) {
        if (response.errorCode() != Errors.NONE.code()) {
            this.mechanism = null;
        }
        return KrpcFilterState.FORWARD;
    }

    @Override
    public KrpcFilterState onSaslAuthenticateRequest(SaslAuthenticateRequestData request,
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
        return KrpcFilterState.FORWARD;
    }

    @Override
    public KrpcFilterState onSaslAuthenticateResponse(SaslAuthenticateResponseData response,
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
        return KrpcFilterState.FORWARD;
    }

}
