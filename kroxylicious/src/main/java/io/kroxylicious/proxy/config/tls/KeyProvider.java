/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.netty.handler.ssl.SslContextBuilder;

/**
 * A KeyProvider is a source of a TLS private-key/certificate pair and optionally intermediate certificate(s).
 * <ul>
 *     <li>In the TLS <em>server</em> role, it is used to provide the certificate presented by the TLS server.
 *     In the server role a KeyProvider is mandatory.</li>
 *     <li>In the TLS <em>client</em> role, it is used for TLS client authentication so that the client can
 *     identify itself to the server.</li>
 * </ul>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({ @JsonSubTypes.Type(KeyPair.class), @JsonSubTypes.Type(KeyStore.class) })
public interface KeyProvider {

    SslContextBuilder forServer();

}
