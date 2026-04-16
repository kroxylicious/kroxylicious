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
 * Declares that a plugin implementation has no mutable state after initialisation,
 * making it safe to share a single instance across multiple consumers.
 *
 * <p>This annotation is placed on plugin implementation classes by the plugin developer.
 * End users control whether an instance is actually shared via the {@code shared} field
 * in the plugin instance YAML. The framework rejects {@code shared: true} on plugins
 * that are not annotated with {@code @Stateless}.
 *
 * @see Plugin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Stateless {
}
