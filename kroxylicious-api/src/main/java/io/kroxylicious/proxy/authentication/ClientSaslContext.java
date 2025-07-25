/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

import java.util.Optional;

import io.kroxylicious.proxy.filter.FilterContext;

/**
 * Exposes SASL authentication information to plugins, for example using {@link FilterContext#clientSaslContext()}.
 * This is implemented by the runtime for use by plugins.
 */
public interface ClientSaslContext {

    /**
     * The name of the SASL mechanism used.
     * @return The name of the SASL mechanism used.
     */
    String mechanismName();

    /**
     * Returns the client's authorizationId that resulted from the SASL exchange.
     * @return the client's authorizationId.
     */
    String authorizationId();
}
