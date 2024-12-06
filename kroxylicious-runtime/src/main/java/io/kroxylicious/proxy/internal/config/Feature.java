/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.config;

import org.slf4j.Logger;

/**
 * Configuration Features related to how we load configuration.
 */
public enum Feature {

    /**
     * Enables loading specific nodes in the configuration file which configure test-only features. These are unsupported
     * for production usages and there are no compatibility guarantees for their configuration model.
     */
    TEST_CONFIGURATION {
        @Override
        public void maybeLogWarning(Logger logger) {
            logger.warn(
                    "test-only configuration for proxy will be loaded. these configurations are unsupported and have no compatibility guarantees, they could be removed or changed at any time");
        }
    };

    /**
     * At proxy initialisation time, if this feature is enabled then this method will be invoked to give the
     * feature an opportunity to log any disclaimers.
     * @param logger logger
     */
    public void maybeLogWarning(Logger logger) {

    }
}
