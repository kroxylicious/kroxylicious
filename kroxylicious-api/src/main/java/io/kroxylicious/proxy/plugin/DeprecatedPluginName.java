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
 * <p>Annotates a {@linkplain Plugin @Plugin}
 * implementation class whose
 * fully-qualified type name has been changed
 * and whose old name should no longer be used
 * to refer to it.</p>
 *
 * <p>Plugin implementations should ideally have a single,
 * canonical name (the fully-qualified class name), so
 * this annotation is not intended to provide a general purpose
 * plugin aliasing facility
 * Instead, it is provided as a way of "renaming a plugin"
 * while maintaining backwards compatibility with
 * configuration files that continue to use the old
 * implementation class name.
 * When a plugin implementation is instantiated using the old name
 * a warning will be logged prompting the
 * end user to update their configuration to use
 * the new name.</p>
 *
 * <p>If a plugin implementation class itself is deprecated then
 * the {@linkplain Deprecated @Deprecated}
 * annotation should be used on that class instead.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DeprecatedPluginName {
    /**
     * The fully qualified class name by which this plugin was previously known
     */
    String oldName();

}
