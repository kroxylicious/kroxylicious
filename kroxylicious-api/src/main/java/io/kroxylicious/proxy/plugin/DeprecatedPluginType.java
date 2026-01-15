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
 * Like {@link PluginImplName}, but for an old plugin interface type that has been {@link Deprecated @Deprecated} and replaced by some new
 * interface type.
 *
 * This the plugin interface equivalent of {@link DeprecatedPluginName @DeprecatedPluginName}.
 * {@link DeprecatedPluginName @DeprecatedPluginName} is used when renaming a plugin implementation class.
 * This annotation is used when renaming a plugin interface class.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface DeprecatedPluginType {
    /**
     * The class reflecting the plugin interface (e.g. {@link io.kroxylicious.proxy.filter.FilterFactory}).
     * @return The class reflecting the plugin interface
     */
    Class<?> value();
}
