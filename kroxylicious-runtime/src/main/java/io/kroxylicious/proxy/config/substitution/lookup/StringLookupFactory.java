/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.substitution.lookup;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import io.kroxylicious.proxy.config.substitution.StringSubstitutor;

/**
 * Create instances of string lookups or access singleton string lookups implemented in this package.
 * <p>
 * The "classic" look up is {@link #mapStringLookup(Map)}.
 * </p>
 * <p>
 * The methods for variable interpolation (A.K.A. variable substitution) are:
 * </p>
 * <ul>
 * <li>{@link #interpolatorStringLookup()}.</li>
 * </ul>
 * <p>
 * Unless explicitly requested otherwise, a set of default lookups are included for convenience with these
 * variable interpolation methods. These defaults are listed in the table below. However, the exact lookups
 * included can be configured through the use of the {@value #DEFAULT_STRING_LOOKUPS_PROPERTY} system property.
 * If present, this system property will be parsed as a comma-separated list of lookup names, with the names
 * being those defined by the {@link DefaultStringLookup} enum. For example, setting this system property to
 * {@code "ENVIRONMENT"} will only include the {@link DefaultStringLookup#ENVIRONMENT ENVIRONMENT}
 * lookup. Setting the property to the empty string will cause no defaults to be configured.
 * </p>
 * <table>
 * <caption>Default String Lookups</caption>
 * <tr>
 * <th>Key</th>
 * <th>Interface</th>
 * <th>Factory Method</th>
 * <th>Since</th>
 * </tr>
 * <tr>
 * <td>{@value #KEY_ENV}</td>
 * <td>{@link StringLookup}</td>
 * <td>{@link #environmentVariableStringLookup()}</td>
 * <td>1.3</td>
 * </tr>
 * <tr>
 * <td>{@value #KEY_SYS}</td>
 * <td>{@link StringLookup}</td>
 * <td>{@link #systemPropertyStringLookup()}</td>
 * <td>1.3</td>
 * </tr>
 * </table>
 *
 */
public final class StringLookupFactory {

    /**
     * Internal class used to construct the default {@link StringLookup} map used by
     * {@link StringLookupFactory#addDefaultStringLookups(Map)}.
     */
    static final class DefaultStringLookupsHolder {

        /** Singleton instance, initialized with the system properties. */
        static final DefaultStringLookupsHolder INSTANCE = new DefaultStringLookupsHolder(System.getProperties());

        /**
         * Add the key and string lookup from {@code lookup} to {@code map}, also adding any additional
         * key aliases if needed. Keys are normalized using the {@link #toKey(String)} method.
         * @param lookup lookup to add
         * @param map map to add to
         */
        private static void addLookup(final DefaultStringLookup lookup, final Map<String, StringLookup> map) {
            map.put(toKey(lookup.getKey()), lookup.getStringLookup());
        }

        /**
         * Create the lookup map used when the user has requested no customization.
         * @return default lookup map
         */
        private static Map<String, StringLookup> createDefaultStringLookups() {
            final Map<String, StringLookup> lookupMap = new HashMap<>();

            addLookup(DefaultStringLookup.ENVIRONMENT, lookupMap);
            addLookup(DefaultStringLookup.SYSTEM_PROPERTIES, lookupMap);

            return lookupMap;
        }

        /**
         * Construct a lookup map by parsing the given string. The string is expected to contain
         * comma or space-separated names of values from the {@link DefaultStringLookup} enum. If
         * the given string is null or empty, an empty map is returned.
         * @param str string to parse; may be null or empty
         * @return lookup map parsed from the given string
         */
        private static Map<String, StringLookup> parseStringLookups(final String str) {
            final Map<String, StringLookup> lookupMap = new HashMap<>();

            try {
                for (final String lookupName : str.split("[\\s,]+")) {
                    if (!lookupName.isEmpty()) {
                        addLookup(DefaultStringLookup.valueOf(lookupName.toUpperCase()), lookupMap);
                    }
                }
            }
            catch (IllegalArgumentException exc) {
                throw new IllegalArgumentException("Invalid default string lookups definition: " + str, exc);
            }

            return lookupMap;
        }

        /** Default string lookup map. */
        private final Map<String, StringLookup> defaultStringLookups;

        /**
         * Construct a new instance initialized with the given properties.
         * @param props initialization properties
         */
        DefaultStringLookupsHolder(final Properties props) {
            final Map<String, StringLookup> lookups = props.containsKey(StringLookupFactory.DEFAULT_STRING_LOOKUPS_PROPERTY)
                    ? parseStringLookups(props.getProperty(StringLookupFactory.DEFAULT_STRING_LOOKUPS_PROPERTY))
                    : createDefaultStringLookups();

            defaultStringLookups = Collections.unmodifiableMap(lookups);
        }

        /**
         * Get the default string lookups map.
         * @return default string lookups map
         */
        Map<String, StringLookup> getDefaultStringLookups() {
            return defaultStringLookups;
        }
    }

    /**
     * Defines the singleton for this class.
     */
    public static final StringLookupFactory INSTANCE = new StringLookupFactory();

    /**
     * Looks up keys from environment variables.
     * <p>
     * Using a {@link StringLookup} from the {@link StringLookupFactory}:
     * </p>
     *
     * <pre>
     * StringLookupFactory.INSTANCE.environmentVariableStringLookup().lookup("USER");
     * </pre>
     * <p>
     * Using a {@link StringSubstitutor}:
     * </p>
     *
     * <pre>
     * StringSubstitutor.createInterpolator().replace("... ${env:USER} ..."));
     * </pre>
     * <p>
     * The above examples convert (on Linux) {@code "USER"} to the current user name. On Windows 10, you would use
     * {@code "USERNAME"} to the same effect.
     * </p>
     */
    static final FunctionStringLookup<String> INSTANCE_ENVIRONMENT_VARIABLES = FunctionStringLookup.on(System::getenv);

    /**
     * Defines the FunctionStringLookup singleton that always returns null.
     */
    static final FunctionStringLookup<String> INSTANCE_NULL = FunctionStringLookup.on(key -> null);

    /**
     * Defines the FunctionStringLookup singleton for looking up system properties.
     */
    static final FunctionStringLookup<String> INSTANCE_SYSTEM_PROPERTIES = FunctionStringLookup.on(System::getProperty);

    /**
     * Default lookup key for interpolation {@value #KEY_ENV}.
     *
     * @since 1.6
     */
    public static final String KEY_ENV = "env";

    /**
     * Default lookup key for interpolation {@value #KEY_SYS}.
     *
     * @since 1.6
     */
    public static final String KEY_SYS = "sys";

    /**
     * Name of the system property used to determine the string lookups added by the
     * {@link #addDefaultStringLookups(Map)} method. Use of this property is only required
     * in cases where the set of default lookups must be modified. (See the class documentation
     * for details.)
     *
     * @since 1.10.0
     */
    public static final String DEFAULT_STRING_LOOKUPS_PROPERTY = "org.apache.commons.text.lookup.StringLookupFactory.defaultStringLookups";

    /**
     * Clears any static resources.
     *
     * @since 1.5
     */
    public static void clear() {
    }

    /**
     * Get a string suitable for use as a key in the string lookup map.
     * @param key string to convert to a string lookup map key
     * @return string lookup map key
     */
    static String toKey(final String key) {
        return key.toLowerCase(Locale.ROOT);
    }

    /**
     * Returns the given map if the input is non-null or an empty immutable map if the input is null.
     *
     * @param <K> the class of the map keys
     * @param <V> the class of the map values
     * @param map The map to test
     * @return the given map if the input is non-null or an empty immutable map if the input is null.
     */
    static <K, V> Map<K, V> toMap(final Map<K, V> map) {
        return map == null ? Collections.emptyMap() : map;
    }

    /**
     * No need to build instances for now.
     */
    private StringLookupFactory() {
        // empty
    }

    /**
     * Adds the default string lookups for this class to {@code stringLookupMap}. The default string
     * lookups are a set of built-in lookups added for convenience during string interpolation. The
     * defaults may be configured using the {@value #DEFAULT_STRING_LOOKUPS_PROPERTY} system property.
     * See the class documentation for details and a list of lookups.
     *
     * @param stringLookupMap the map of string lookups to edit.
     * @since 1.5
     */
    public void addDefaultStringLookups(final Map<String, StringLookup> stringLookupMap) {
        if (stringLookupMap != null) {
            stringLookupMap.putAll(DefaultStringLookupsHolder.INSTANCE.getDefaultStringLookups());
        }
    }

    /**
     * Returns the EnvironmentVariableStringLookup singleton instance where the lookup key is an environment variable
     * name.
     * <p>
     * Using a {@link StringLookup} from the {@link StringLookupFactory}:
     * </p>
     *
     * <pre>
     * StringLookupFactory.INSTANCE.environmentVariableStringLookup().lookup("USER");
     * </pre>
     * <p>
     * Using a {@link StringSubstitutor}:
     * </p>
     *
     * <pre>
     * StringSubstitutor.createInterpolator().replace("... ${env:USER} ..."));
     * </pre>
     * <p>
     * The above examples convert (on Linux) {@code "USER"} to the current user name. On Windows 10, you would use
     * {@code "USERNAME"} to the same effect.
     * </p>
     *
     * @return The EnvironmentVariableStringLookup singleton instance.
     */
    public StringLookup environmentVariableStringLookup() {
        return StringLookupFactory.INSTANCE_ENVIRONMENT_VARIABLES;
    }

    /**
     * Returns a {@link InterpolatorStringLookup} containing the configured
     * {@link #addDefaultStringLookups(Map) default lookups}. See the class documentation for
     * details on how these defaults are configured.
     * <p>
     * Using a {@link org.apache.commons.text.lookup.StringLookup} from the {@link org.apache.commons.text.lookup.StringLookupFactory}:
     * </p>
     *
     * <pre>
     * StringLookupFactory.INSTANCE.interpolatorStringLookup().lookup("${sys:os.name}, ${env:USER}");
     * </pre>
     * <p>
     * Using a {@link StringSubstitutor}:
     * </p>
     *
     * <pre>
     * StringSubstitutor.createInterpolator().replace("... ${sys:os.name}, ${env:USER} ..."));
     * </pre>
     * <p>
     * The above examples convert {@code "${sys:os.name}, ${env:USER}"} to the OS name and Linux user name.
     * </p>
     *
     * @return the default {@link InterpolatorStringLookup}.
     */
    public StringLookup interpolatorStringLookup() {
        return InterpolatorStringLookup.INSTANCE;
    }

    /**
     * Returns a new map-based lookup where the request for a lookup is answered with the value for that key.
     *
     * @param <V> the map value type.
     * @param map the map.
     * @return a new MapStringLookup.
     */
    public <V> StringLookup mapStringLookup(final Map<String, V> map) {
        return FunctionStringLookup.on(map);
    }

    /**
     * Returns the NullStringLookup singleton instance which always returns null.
     *
     * @return The NullStringLookup singleton instance.
     */
    public StringLookup nullStringLookup() {
        return StringLookupFactory.INSTANCE_NULL;
    }

    /**
     * Returns the SystemPropertyStringLookup singleton instance where the lookup key is a system property name.
     *
     * <p>
     * Using a {@link StringLookup} from the {@link StringLookupFactory}:
     * </p>
     *
     * <pre>
     * StringLookupFactory.INSTANCE.systemPropertyStringLookup().lookup("os.name");
     * </pre>
     * <p>
     * Using a {@link StringSubstitutor}:
     * </p>
     *
     * <pre>
     * StringSubstitutor.createInterpolator().replace("... ${sys:os.name} ..."));
     * </pre>
     * <p>
     * The above examples convert {@code "os.name"} to the operating system name.
     * </p>
     *
     * @return The SystemPropertyStringLookup singleton instance.
     */
    public StringLookup systemPropertyStringLookup() {
        return StringLookupFactory.INSTANCE_SYSTEM_PROPERTIES;
    }

}
