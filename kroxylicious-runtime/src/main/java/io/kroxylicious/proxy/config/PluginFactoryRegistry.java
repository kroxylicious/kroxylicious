/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface PluginFactoryRegistry {

    /**
     * Resolve a plugin interface type to a factory for that type of plugin
     * @param pluginClass The plugin interface type.
     * @return The factory for that type of plugin
     * @param <P> The type of plugin
     */
    <P> @NonNull PluginFactory<P> pluginFactory(
            @NonNull
            Class<P> pluginClass
    );
}
