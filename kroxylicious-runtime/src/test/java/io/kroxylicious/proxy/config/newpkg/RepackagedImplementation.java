/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.newpkg;

import io.kroxylicious.proxy.config.ServiceWithBaggage;
import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = Void.class)
@DeprecatedPluginName(oldName = "io.kroxylicious.proxy.config.oldpkg.RepackagedImplementation")
public class RepackagedImplementation implements ServiceWithBaggage {
}
