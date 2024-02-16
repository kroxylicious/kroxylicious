/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.substitution.lookup;

/**
 * An enumeration defining {@link StringLookup} objects available through {@link StringLookupFactory}.
 * <p>
 * This enum was adapted and expanded from Apache Commons Configuration 2.4.
 * </p>
 * <p><strong>NOTE:</strong> Starting in version 1.10.0, not all lookups defined in this class are
 * included by default in the
 * {@link StringLookupFactory#addDefaultStringLookups(java.util.Map) StringLookupFactory.addDefaultStringLookups}
 * method. See the {@link StringLookupFactory} class documentation for details.
 * </p>
 *
 * @see StringLookupFactory
 * @see StringLookup
 * @since 1.7
 */
public enum DefaultStringLookup {

    /**
     * The lookup for environment properties using the key {@code "env"}.
     * @see StringLookupFactory#KEY_ENV
     * @see StringLookupFactory#environmentVariableStringLookup()
     */
    ENVIRONMENT(org.apache.commons.text.lookup.StringLookupFactory.KEY_ENV, StringLookupFactory.INSTANCE.environmentVariableStringLookup()),

    /**
     * The lookup for system properties using the key {@code "sys"}.
     * @see StringLookupFactory#KEY_SYS
     * @see StringLookupFactory#systemPropertyStringLookup()
     */
    SYSTEM_PROPERTIES(StringLookupFactory.KEY_SYS, StringLookupFactory.INSTANCE.systemPropertyStringLookup());

    /** The prefix under which the associated lookup object is registered. */
    private final String key;

    /** The associated lookup instance. */
    private final StringLookup lookup;

    /**
     * Creates a new instance of {@link DefaultStringLookup} and sets the key and the associated lookup instance.
     *
     * @param prefix the prefix
     * @param lookup the {@link org.apache.commons.text.lookup.StringLookup} instance
     */
    DefaultStringLookup(final String prefix, final StringLookup lookup) {
        this.key = prefix;
        this.lookup = lookup;
    }

    /**
     * Returns the standard prefix for the lookup object of this kind.
     *
     * @return the prefix
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the standard {@link StringLookup} instance of this kind.
     *
     * @return the associated {@link StringLookup} object
     */
    public StringLookup getStringLookup() {
        return lookup;
    }
}
