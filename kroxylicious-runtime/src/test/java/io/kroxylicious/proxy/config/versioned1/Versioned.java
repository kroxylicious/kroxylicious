/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.versioned1;

import io.kroxylicious.proxy.config.ServiceWithVersionedImpls;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Version;

@Version("v1")
@Plugin(configType = String.class)
public class Versioned implements ServiceWithVersionedImpls {
}
