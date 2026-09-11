/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

/**
 * A named entity.
 */
public interface Named {
    /**
     * The name of the entity.
     * @return name.
     */
    String name();
}
