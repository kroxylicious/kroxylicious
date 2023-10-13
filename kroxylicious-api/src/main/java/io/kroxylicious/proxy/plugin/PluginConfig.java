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
 * This should be applied to the property of class representing the plugin point, and point to
 * the name of the sibling property (annotated with {@link PluginReference @PluginReference}) that names the plugin instance.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface PluginConfig {

    /**
     * @return The name of the property which holds the plugin instance name to which this config relates.
     */
    String instanceNameProperty();
}
