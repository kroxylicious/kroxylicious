/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.re2j.Pattern;

/**
 * Resource names selected by a set of ACL rules, in the forms the rules use: any name, exact names,
 * name prefixes, and regular expressions (RE2 syntax, matching the whole name).
 *
 * <p>The regular expressions are compiled once, on construction, so {@link #matches(String)} does not
 * compile them again.</p>
 */
public final class ResourceNameSelectors {

    /** Selects no name. */
    public static final ResourceNameSelectors NONE = new ResourceNameSelectors(false, Set.of(), Set.of(), Set.of());

    private final boolean anyName;
    private final Set<String> names;
    private final Set<String> prefixes;
    private final Set<String> patterns;
    private final List<Pattern> compiledPatterns;

    /**
     * Constructs a ResourceNameSelectors.
     * @param anyName true if every name is selected.
     * @param names the names selected exactly.
     * @param prefixes the prefixes of the names selected.
     * @param patterns the regular expressions of the names selected.
     * @throws com.google.re2j.PatternSyntaxException if a pattern is not a valid RE2 regular expression.
     */
    public ResourceNameSelectors(boolean anyName, Set<String> names, Set<String> prefixes, Set<String> patterns) {
        this.anyName = anyName;
        this.names = Set.copyOf(Objects.requireNonNull(names));
        this.prefixes = Set.copyOf(Objects.requireNonNull(prefixes));
        this.patterns = Set.copyOf(Objects.requireNonNull(patterns));
        this.compiledPatterns = this.patterns.stream().map(Pattern::compile).toList();
    }

    /**
     * Returns true if every name is selected.
     * @return true if every name is selected.
     */
    public boolean anyName() {
        return anyName;
    }

    /**
     * Returns the names selected exactly.
     * @return the names selected exactly.
     */
    public Set<String> names() {
        return names;
    }

    /**
     * Returns the prefixes of the names selected.
     * @return the prefixes of the names selected.
     */
    public Set<String> prefixes() {
        return prefixes;
    }

    /**
     * Returns the regular expressions of the names selected.
     * @return the regular expressions of the names selected.
     */
    public Set<String> patterns() {
        return patterns;
    }

    /**
     * Returns true if the name is selected.
     * @param name a resource name.
     * @return true if the name is selected.
     */
    public boolean matches(String name) {
        if (anyName || names.contains(name)) {
            return true;
        }
        for (String prefix : prefixes) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        for (Pattern pattern : compiledPatterns) {
            if (pattern.matches(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if nothing is selected.
     * @return true if nothing is selected.
     */
    public boolean isEmpty() {
        return !anyName && names.isEmpty() && prefixes.isEmpty() && patterns.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ResourceNameSelectors that
                && anyName == that.anyName
                && names.equals(that.names)
                && prefixes.equals(that.prefixes)
                && patterns.equals(that.patterns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(anyName, names, prefixes, patterns);
    }

    @Override
    public String toString() {
        return "ResourceNameSelectors[anyName=" + anyName + ", names=" + names + ", prefixes=" + prefixes
                + ", patterns=" + patterns + "]";
    }
}
