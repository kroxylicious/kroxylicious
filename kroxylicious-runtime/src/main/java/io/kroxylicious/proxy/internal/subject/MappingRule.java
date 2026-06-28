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

interface MappingRule extends Function<String, Optional<String>> {

    String ELSE_IDENTITY = "identity";
    String ELSE_ANONYMOUS = "anonymous";

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
