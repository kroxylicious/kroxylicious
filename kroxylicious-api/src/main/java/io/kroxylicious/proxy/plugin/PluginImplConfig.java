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
 * An annotation that identifies a plugin instance name at a plugin point within the configuration.
 * This should be applied to the property of the class representing the plugin point, and should name the
 * corresponding {@link PluginImplName @PluginReference}-annotated sibling property.
 * @see io.kroxylicious.proxy.plugin
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface PluginImplConfig {

    /**
     * @return The name of the {@link PluginImplName @PluginReference}-annotated sibling property.
     */
    String implNameProperty();
}
