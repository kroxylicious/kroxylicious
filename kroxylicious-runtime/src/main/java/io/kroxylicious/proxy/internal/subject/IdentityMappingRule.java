/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.Optional;

public class IdentityMappingRule implements MappingRule {
    @Override
    public Optional<String> apply(String s) {
        return Optional.of(s);
    }
}
