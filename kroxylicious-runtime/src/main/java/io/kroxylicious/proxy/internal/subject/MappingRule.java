/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A rule which transforms a name extracted from authentication state into the name of a principal.
 * Applying a rule yields an empty result if the rule does not match the given name.
 */
interface MappingRule extends Function<String, Optional<String>> {

    /** {@code else} value that passes the name through unchanged. */
    String ELSE_IDENTITY = "identity";
    /** {@code else} value that discards the name, contributing no principal. */
    String ELSE_ANONYMOUS = "anonymous";

    /**
     * Builds the mapping rules corresponding to the given {@code map} configurations, validating that
     * an {@code else} mapping occurs at most once and only as the last element.
     * A null or empty list yields a single {@link IdentityMappingRule}.
     *
     * @param maps the {@code map} configurations, possibly null.
     * @return the corresponding mapping rules.
     */
    static List<MappingRule> buildMappingRules(@Nullable List<Map> maps) {
        if (maps == null || maps.isEmpty()) {
            return List.of(new IdentityMappingRule());
        }
        int firstElseIndex = -1;
        int numElses = 0;
        for (int i = 0; i < maps.size(); i++) {
            Map m = maps.get(i);

            if (m.else_() != null) {
                numElses++;
                if (firstElseIndex == -1) {
                    firstElseIndex = i;
                }
            }
        }
        if (numElses > 1) {
            throw new IllegalArgumentException("An `else` mapping may only occur at most once, as the last element of `map`.");
        }
        else if (firstElseIndex != -1 && firstElseIndex < maps.size() - 1) {
            throw new IllegalArgumentException("An `else` mapping may only occur as the last element of `map`.");
        }
        return maps.stream().map(MappingRule::buildMappingRule).toList();
    }

    private static MappingRule buildMappingRule(Map map) {
        if (map.replaceMatch() != null) {
            return new ReplaceMatchMappingRule(map.replaceMatch());
        }
        else if (ELSE_IDENTITY.equals(map.else_())) {
            return new IdentityMappingRule();
        }
        else if (ELSE_ANONYMOUS.equals(map.else_())) {
            return s -> Optional.empty();
        }
        else {
            throw new IllegalArgumentException("Unknown `else` map '%s', supported values are: '%s', '%s'."
                    .formatted(map.else_(), ELSE_IDENTITY, ELSE_ANONYMOUS));
        }
    }

}
