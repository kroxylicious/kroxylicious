/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation, on a plugin implementation class, that identifies the class of "config record"
 * consumed by that implementation.
 * Use {@code @Plugin(configType=Void.class)} if a plugin implementation class doesn't require configuration.
 * @see io.kroxylicious.proxy.plugin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Plugin.List.class)
public @interface Plugin {

    /**
     * @return The version of the configuration associated with the plugin implementation.
     */
    String configVersion() default "";

    /**
     * @return The type of configuration associated with the plugin implementation.
     */
    Class<?> configType();

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface List {
        Plugin[] value();
    }
}
