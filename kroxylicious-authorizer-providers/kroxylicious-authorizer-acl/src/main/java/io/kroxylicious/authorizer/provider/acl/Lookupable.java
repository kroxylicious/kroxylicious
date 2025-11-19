/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import edu.umd.cs.findbugs.annotations.Nullable;

interface Lookupable<T> {
    Class<? extends T> type();

    @Nullable
    TypeNameMap.Predicate predicate();

    @Nullable
    String operand();
}
