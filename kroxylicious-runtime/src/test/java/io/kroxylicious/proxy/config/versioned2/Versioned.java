/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.versioned2;

import io.kroxylicious.proxy.config.ServiceWithVersionedImpls;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Version;

@Version("v2")
@Plugin(configType = Integer.class)
public class Versioned implements ServiceWithVersionedImpls {
}
