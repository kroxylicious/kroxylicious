/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.Objects;
import java.util.Set;

import com.google.re2j.Pattern;

/**
 * Resource names selected by a set of ACL rules, in the forms the rules use: any name, exact names,
 * name prefixes, and regular expressions (RE2 syntax, matching the whole name).
 *
 * @param anyName true if every name is selected.
 * @param names the names selected exactly.
 * @param prefixes the prefixes of the names selected.
 * @param patterns the regular expressions of the names selected.
 */
public record ResourceNameSelectors(boolean anyName, Set<String> names, Set<String> prefixes, Set<String> patterns) {

    /** Selects no name. */
    public static final ResourceNameSelectors NONE = new ResourceNameSelectors(false, Set.of(), Set.of(), Set.of());

    /**
     * Constructs a ResourceNameSelectors.
     * @param anyName true if every name is selected.
     * @param names the names selected exactly.
     * @param prefixes the prefixes of the names selected.
     * @param patterns the regular expressions of the names selected.
     */
    public ResourceNameSelectors {
        names = Set.copyOf(Objects.requireNonNull(names));
        prefixes = Set.copyOf(Objects.requireNonNull(prefixes));
        patterns = Set.copyOf(Objects.requireNonNull(patterns));
    }

    /**
     * Returns true if the name is selected.
     * @param name a resource name.
     * @return true if the name is selected.
     */
    public boolean matches(String name) {
        return anyName
                || names.contains(name)
                || prefixes.stream().anyMatch(name::startsWith)
                || patterns.stream().anyMatch(p -> Pattern.compile(p).matches(name));
    }

    /**
     * Returns true if nothing is selected.
     * @return true if nothing is selected.
     */
    public boolean isEmpty() {
        return !anyName && names.isEmpty() && prefixes.isEmpty() && patterns.isEmpty();
    }
}
