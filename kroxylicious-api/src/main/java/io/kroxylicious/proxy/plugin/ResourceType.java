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
 * Declares that a plugin implementation's data is non-YAML (e.g. binary keystore bytes).
 * The runtime uses the specified serialiser and deserialiser to convert between raw bytes
 * and the plugin's typed configuration, rather than parsing the data as YAML.
 *
 * <p>Plugins without this annotation have their data interpreted as YAML by default.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ResourceType {

    /**
     * @return the serialiser class used to convert a typed resource to bytes (for snapshot generation).
     */
    Class<? extends ResourceSerializer<?>> serializer();

    /**
     * @return the deserialiser class used to convert bytes to a typed resource (for loading).
     */
    Class<? extends ResourceDeserializer<?>> deserializer();
}
