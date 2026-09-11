/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;

class FilterDirectIT extends AbstractFilterIT {

    @Override
    protected ConfigurationBuilder proxyConfig(String bootstrapServers) {
        return KroxyliciousConfigUtils.proxy(bootstrapServers);
    }
}
