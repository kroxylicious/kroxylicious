/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Options that control how trust is applied during TLS negotiation.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({ @JsonSubTypes.Type(ServerOptions.class) })
public interface TrustOptions {
    /**
     * Indicates whether these options apply to the TLS client role.
     * @return true if these options apply to the TLS client role, false if they apply to the TLS server role
     */
    boolean forClient();
}
