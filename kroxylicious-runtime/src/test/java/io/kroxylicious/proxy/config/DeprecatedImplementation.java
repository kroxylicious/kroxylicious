/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = Void.class)
@Deprecated(forRemoval = true) // not really! This class exists to test how deprecated plugins are handled
public class DeprecatedImplementation implements ServiceWithBaggage {
}
