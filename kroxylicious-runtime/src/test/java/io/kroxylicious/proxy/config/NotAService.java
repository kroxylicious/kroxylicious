/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import io.kroxylicious.proxy.plugin.ApiVersion;

/**
 * This interface is not a service interface (no META-INF/services file). Used for tests
 */
@ApiVersion("v1")
public interface NotAService {
}
