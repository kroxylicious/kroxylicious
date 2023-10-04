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
 * Annotates a reference, by name, to a plugin interface at a plugin point within the configuration.
 * This annotation is applied to a property of a "config class" (i.e. the Java class representing the JSON config definition).,
 * The {@link #value()} points to the plugin interface (e.g. {@link io.kroxylicious.proxy.filter.FilterFactory}.
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
