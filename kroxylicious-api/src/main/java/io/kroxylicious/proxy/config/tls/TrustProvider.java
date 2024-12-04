/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A TrustProvider is a source of trust anchors used to determine whether a certificate present by a peer is trusted.
 * <ul>
 *     <li>In the TLS <em>client</em> role, it is used to validate that the server's certificate is trusted.  If the
 *     trust provider is omitted platform trust is used instead.</li>
 *     <li>In the TLS <em>server</em> role, when the TLS client authentication is in use, it  is used by the server to
 *     ensure that the client's certificate is known.</li>
 * </ul>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({ @JsonSubTypes.Type(TrustStore.class), @JsonSubTypes.Type(InsecureTls.class) })
public interface TrustProvider {

    /**
     * Visits the trust provider {@link TrustProviderVisitor}. Implementor should call one `visit` method on visitor.
     * @param visitor visitor.
     */
    <T> T accept(TrustProviderVisitor<T> visitor);

    /**
     * Trust options that apply to this TLS peer..
     *
     * @return trust options
     */
    default @Nullable TrustOptions trustOptions() {
        return null;
    }
}
