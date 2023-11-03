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
 * An annotation, on a plugin implementation class, that identifies the class of "config record"
 * consumed by that implementation.
 * Use {@code @Plugin(configType=Void.class)} if a plugin implementation class doesn't require configuration.
 * @see io.kroxylicious.proxy.plugin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Plugin {
    Class<?> configType();
}
