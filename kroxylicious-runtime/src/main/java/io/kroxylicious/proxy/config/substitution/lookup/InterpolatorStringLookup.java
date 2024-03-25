/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.substitution.lookup;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Proxies other {@link StringLookup}s using a keys within ${} markers using the format "${StringLookup:Key}".
 */
class InterpolatorStringLookup implements StringLookup {

    /** Constant for the prefix separator. */
    private static final char PREFIX_SEPARATOR = ':';

    /** The map of String lookups keyed by prefix. */
    private final Map<String, StringLookup> stringLookupMap;

    /**
     * Constructs a fully customized instance.
     *
     * @param stringLookupMap the map of string lookups.
     */
    InterpolatorStringLookup(final Map<String, StringLookup> stringLookupMap) {
        Objects.requireNonNull(stringLookupMap);
        this.stringLookupMap = Map.copyOf(stringLookupMap);
    }

    /**
     * Resolves the specified variable. This implementation will try to extract a variable prefix from the given
     * variable name (the first colon (':') is used as prefix separator). It then passes the name of the variable with
     * the prefix stripped to the lookup object registered for this prefix. If no prefix can be found or if the
     * associated lookup object cannot resolve this variable, the default lookup object will be used.
     *
     * @param key the name of the variable whose value is to be looked up
     * @return The value of this variable or <b>null</b> if it cannot be resolved
     */
    @Override
    public String lookup(String key) {
        if (key == null) {
            return null;
        }

        final int prefixPos = key.indexOf(PREFIX_SEPARATOR);
        if (prefixPos >= 0) {
            final String prefix = key.substring(0, prefixPos).toLowerCase(Locale.ROOT);
            final String name = key.substring(prefixPos + 1);
            final StringLookup lookup = stringLookupMap.get(prefix);
            String value = null;
            if (lookup != null) {
                value = lookup.lookup(name);
            }

            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return super.toString() + " [stringLookupMap=" + stringLookupMap + "]";
    }
}
