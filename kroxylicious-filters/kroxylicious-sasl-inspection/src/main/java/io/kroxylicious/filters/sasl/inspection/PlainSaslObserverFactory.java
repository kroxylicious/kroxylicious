/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import io.kroxylicious.proxy.plugin.Plugin;

/**
 * Factory for the {@link PlainSaslObserver} instances.
 * <br/>
 * The PLAIN mechanism relies on cleartext password being passed between client
 * and server. THis means the cleartext password will exist in the memory of the
 * Proxy.
 */
@Plugin(configType = Void.class)
public class PlainSaslObserverFactory implements SaslObserverFactory {
    @Override
    public SaslObserver createObserver() {
        return new PlainSaslObserver();
    }

    @Override
    public String mechanismName() {
        return "PLAIN";
    }

    @Override
    public boolean transmitsCredentialInCleartext() {
        return true;
    }
}
