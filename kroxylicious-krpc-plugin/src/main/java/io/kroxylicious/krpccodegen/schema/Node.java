/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.util.List;

public record Node(FieldSpec field, List<Node> containers, List<Short> orderedVersions) {
    public boolean hasAtLeastOneEntityField() {
        throw new UnsupportedOperationException();
        // return !(entities.isEmpty() && containers.isEmpty());
    }

}
