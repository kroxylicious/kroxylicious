/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.Optional;

/**
 * A {@link MappingRule} which matches every name, mapping it to itself.
 */
public class IdentityMappingRule implements MappingRule {

    /**
     * Creates a rule that maps every name to itself.
     */
    public IdentityMappingRule() {
        // Intentionally empty
    }

    @Override
    public Optional<String> apply(String s) {
        return Optional.of(s);
    }
}
