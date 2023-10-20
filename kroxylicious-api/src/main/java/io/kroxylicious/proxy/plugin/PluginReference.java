/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a property (within a "config record") that names a plugin implementation with the plugin interface.
 * @see io.kroxylicious.proxy.plugin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER })
public @interface PluginReference {
    /**
     * The class reflecting the plugin interface (e.g. {@link io.kroxylicious.proxy.filter.FilterFactory}).
     * @return The class reflecting the plugin interface
     */
    Class<?> value();
}
