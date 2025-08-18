/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.labels.source;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

public interface LabelSourceFactoryContext {
    /**
     * Gets a plugin instance for the given plugin type and name
     * @param pluginClass The plugin type
     * @param instanceName The plugin instance name
     * @return The plugin instance
     * @param <P> The plugin manager type
     * @throws UnknownPluginInstanceException
     */
    <P> P pluginInstance(Class<P> pluginClass, String instanceName);
}
