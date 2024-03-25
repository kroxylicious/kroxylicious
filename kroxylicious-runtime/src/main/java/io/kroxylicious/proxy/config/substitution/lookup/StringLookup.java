/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.substitution.lookup;

/**
 * Lookups a String key for a String value.
 * <p>
 * This class represents the simplest form of a string to string map. It has a benefit over a map in that it can create
 * the result on demand based on the key.
 * </p>
 * <p>
 * For example, it would be possible to implement a lookup that used the key as a primary key, and looked up the value
 * on demand from the database.
 * </p>
 */
@FunctionalInterface
public interface StringLookup {

    /**
     * Looks up a String key to provide a String value.
     * <p>
     * The internal implementation may use any mechanism to return the value. The simplest implementation is to use a
     * Map. However, virtually any implementation is possible.
     * </p>
     * <p>
     * For example, it would be possible to implement a lookup that used the key as a primary key, and looked up the
     * value on demand from the database Or, a numeric based implementation could be created that treats the key as an
     * integer, increments the value and return the result as a string - converting 1 to 2, 15 to 16 etc.
     * </p>
     * <p>
     * This method always returns a String, regardless of the underlying data, by converting it as necessary. For
     * example:
     * </p>
     *
     * <pre>
     * Map&lt;String, Object&gt; map = new HashMap&lt;String, Object&gt;();
     * map.put("number", new Integer(2));
     * assertEquals("2", StringLookupFactory.mapStringLookup(map).lookup("number"));
     * </pre>
     *
     * @param key the key to look up, may be null.
     * @return The matching value, null if no match.
     */
    String lookup(String key);
}
