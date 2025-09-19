/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization;

public record Resource<O extends Enum<O> & Operation<O>>(
        Class<O> reourceType,
        String resorceName) {
}
