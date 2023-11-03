/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.ambiguous2;

import io.kroxylicious.proxy.config.ServiceWithAmbiguousImpls;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = String.class)
public class Ambiguous implements ServiceWithAmbiguousImpls {
}
