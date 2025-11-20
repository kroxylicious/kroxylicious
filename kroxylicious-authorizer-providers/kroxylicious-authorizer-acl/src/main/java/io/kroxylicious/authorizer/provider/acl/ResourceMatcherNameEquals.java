/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.Objects;

record ResourceMatcherNameEquals<T>(Class<? extends T> type, String operand) implements OrderedKey<T> {

    ResourceMatcherNameEquals {
        Objects.requireNonNull(type);
        Objects.requireNonNull(operand);
    }

}
