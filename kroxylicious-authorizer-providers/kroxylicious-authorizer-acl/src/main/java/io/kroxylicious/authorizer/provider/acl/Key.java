/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

sealed interface Key<T> permits OrderedKey, ResourceMatcherNameMatches {
    Class<? extends T> type();
}
