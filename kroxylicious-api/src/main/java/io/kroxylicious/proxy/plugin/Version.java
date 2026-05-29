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
 * Annotation that declares the API version of a plugin implementation.
 * <p>
 * When a plugin implementation is annotated with {@code @Version}, users can reference
 * it in configuration using the format {@code PluginName/version}. This enables:
 * <ul>
 * <li>Explicit API stability guarantees per plugin (independent of project version)</li>
 * <li>Version-based disambiguation when multiple plugins share the same simple class name</li>
 * <li>Migration scenarios where multiple versions coexist in the same JAR</li>
 * </ul>
 * <p>
 * Version identifiers should follow Kubernetes-style versioning conventions:
 * <ul>
 * <li>{@code v1alpha1}, {@code v1alpha2}, etc. - Early alpha versions with no stability guarantees</li>
 * <li>{@code v1beta1}, {@code v1beta2}, etc. - Beta versions with some stability guarantees</li>
 * <li>{@code v1}, {@code v2}, etc. - Stable versions with full backward compatibility within the major version</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * @Plugin(configType = RecordEncryptionConfig.class)
 * @Version("v1alpha1")
 * public class RecordEncryption implements FilterFactory<...> {
 *     // implementation
 * }
 * }</pre>
 * <p>
 * Configuration reference:
 * <pre>{@code
 * filterDefinitions:
 *   - name: encryption
 *     type: RecordEncryption/v1alpha1
 *     config: {}
 * }</pre>
 *
 * @see Plugin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Version {
    /**
     * The API version identifier for this plugin implementation.
     *
     * @return the version identifier (e.g., "v1alpha1", "v1beta1", "v1")
     */
    String value();
}
