/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import io.kroxylicious.proxy.plugin.Plugin;

/**
 * Factory for the {@link OauthBearerSaslObserver} instances.
 */
@Plugin(configType = Void.class)
public class OauthBearerSaslObserverFactory implements SaslObserverFactory {
    @Override
    public SaslObserver createObserver() {
        return new OauthBearerSaslObserver();
    }

    @Override
    public String mechanismName() {
        return "OAUTHBEARER";
    }
}
