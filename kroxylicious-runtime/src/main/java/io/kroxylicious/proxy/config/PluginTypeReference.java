/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Represents a parsed plugin type reference from configuration.
 * <p>
 * Plugin references can include an optional version suffix separated by a forward slash:
 * <ul>
 * <li>{@code RecordEncryption} - Simple plugin name without version</li>
 * <li>{@code RecordEncryption/v1alpha1} - Plugin name with version</li>
 * <li>{@code com.example.RecordEncryption} - Fully qualified name without version</li>
 * <li>{@code com.example.RecordEncryption/v1} - Fully qualified name with version</li>
 * </ul>
 * <p>
 * The version, when present, serves two purposes:
 * <ol>
 * <li>Validates that the configuration matches the plugin's declared API version</li>
 * <li>Disambiguates between multiple plugin implementations with the same simple class name</li>
 * </ol>
 *
 * @param pluginName The plugin name (simple or fully qualified), without version suffix
 * @param version The version identifier (e.g., "v1alpha1"), or null if no version was specified
 */
public record PluginTypeReference(String pluginName, @Nullable String version) {

    public PluginTypeReference {
        Objects.requireNonNull(pluginName);
    }

    /**
     * Parses a plugin type string into a structured reference.
     * <p>
     * The type string is split on the last occurrence of {@code /}, treating everything
     * before as the plugin name and everything after as the version. If no {@code /} is
     * found, the entire string is treated as the plugin name with no version.
     *
     * @param typeString the type string from configuration
     * @return the parsed reference
     * @throws NullPointerException if typeString is null
     */
    public static PluginTypeReference parse(String typeString) {
        Objects.requireNonNull(typeString);
        int lastSlash = typeString.lastIndexOf('/');
        if (lastSlash == -1) {
            return new PluginTypeReference(typeString, null);
        }
        String name = typeString.substring(0, lastSlash);
        String version = typeString.substring(lastSlash + 1);
        return new PluginTypeReference(name, version);
    }

    /**
     * Checks whether this reference includes a version.
     *
     * @return true if version is present, false otherwise
     */
    public boolean hasVersion() {
        return version != null;
    }

    /**
     * Reconstructs the original type string.
     *
     * @return the type string (e.g., "RecordEncryption/v1alpha1" or "RecordEncryption")
     */
    public String toTypeString() {
        return hasVersion() ? pluginName + "/" + version : pluginName;
    }
}
