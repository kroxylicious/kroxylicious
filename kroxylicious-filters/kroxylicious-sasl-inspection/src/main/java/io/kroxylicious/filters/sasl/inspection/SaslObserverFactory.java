/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

/**
 * Factory for {@link SaslObserver} instances.
 */
public interface SaslObserverFactory {
    /**
     * Creates the SASL Observer.
     * @return sasl observer instance
     */
    SaslObserver createObserver();

    /**
     * Returns the <a href="https://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml">IANA registered SASL mechanism name</a>.
     * @return mechanism
     */
    String mechanismName();

    /**
     * Returns true if this SASL mechanisms transmit credentials in the clear.
     *
     * @return true if considered insecure, false otherwise.
     */
    default boolean transmitsCredentialInCleartext() {
        return false;
    }
}
